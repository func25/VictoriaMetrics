package promql

import (
	"flag"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metricsql"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/querystats"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

var (
	maxResponseSeries = flag.Int("search.maxResponseSeries", 0, "The maximum number of time series which can be returned from /api/v1/query and /api/v1/query_range . "+
		"The limit is disabled if it equals to 0. See also -search.maxPointsPerTimeseries and -search.maxUniqueTimeseries")
	treatDotsAsIsInRegexps = flag.Bool("search.treatDotsAsIsInRegexps", false, "Whether to treat dots as is in regexp label filters used in queries. "+
		`For example, foo{bar=~"a.b.c"} will be automatically converted to foo{bar=~"a\\.b\\.c"}, i.e. all the dots in regexp filters will be automatically escaped `+
		`in order to match only dot char instead of matching any char. Dots in ".+", ".*" and ".{n}" regexps aren't escaped. `+
		`This option is DEPRECATED in favor of {__graphite__="a.*.c"} syntax for selecting metrics matching the given Graphite metrics filter`)
	disableImplicitConversion = flag.Bool("search.disableImplicitConversion", false, "Whether to return an error for queries that rely on implicit subquery conversions, "+
		"see https://docs.victoriametrics.com/metricsql/#subqueries for details. "+
		"See also -search.logImplicitConversion.")
	logImplicitConversion = flag.Bool("search.logImplicitConversion", false, "Whether to log queries with implicit subquery conversions, "+
		"see https://docs.victoriametrics.com/metricsql/#subqueries for details. "+
		"Such conversion can be disabled using -search.disableImplicitConversion.")
)

// Exec thực thi một truy vấn PromQL với cấu hình được cung cấp.
// Các bước thực hiện:
// 1. Ghi nhận thống kê query nếu được bật
// 2. Parse và optimize query
// 3. Thực thi query và xử lý kết quả
// 4. Áp dụng các điều chỉnh cuối cùng (first point only, sorting, rounding)
//
// Tham số:
// - qt: Query tracer để theo dõi quá trình thực thi
// - ec: Cấu hình đánh giá query (thời gian, bước, giới hạn, etc.)
// - q: Chuỗi truy vấn PromQL
// - isFirstPointOnly: Chỉ lấy điểm dữ liệu đầu tiên của mỗi time series
func Exec(qt *querytracer.Tracer, ec *EvalConfig, q string, isFirstPointOnly bool) ([]netstorage.Result, error) {
	// Ghi nhận thống kê thời gian thực thi nếu tính năng này được bật
	if querystats.Enabled() {
		startTime := time.Now()
		defer func() {
			ec.QueryStats.addExecutionTimeMsec(startTime)
			if ec.IsMultiTenant {
				querystats.RegisterQueryMultiTenant(q, ec.End-ec.Start, startTime)
				return
			}
			at := ec.AuthTokens[0]
			querystats.RegisterQuery(at.AccountID, at.ProjectID, q, ec.End-ec.Start, startTime)
		}()
	}

	// Validate cấu hình đánh giá
	ec.validate()

	// Parse query với cache để tối ưu hiệu năng
	e, err := parsePromQLWithCache(q)
	if err != nil {
		return nil, err
	}

	// Kiểm tra và xử lý implicit conversion trong query
	// Implicit conversion là quá trình tự động chuyển đổi kiểu dữ liệu không tường minh
	// VD: sum(rate(metric)) -> sum(rate(metric[1m]))
	if *disableImplicitConversion || *logImplicitConversion {
		isInvalid := metricsql.IsLikelyInvalid(e)
		if isInvalid && *disableImplicitConversion {
			return nil, fmt.Errorf("query requires implicit conversion and is rejected according to -search.disableImplicitConversion command-line flag. " +
				"See https://docs.victoriametrics.com/metricsql/#implicit-query-conversions for details")
		}
		if isInvalid && *logImplicitConversion {
			logger.Warnf("query=%q requires implicit conversion, see https://docs.victoriametrics.com/metricsql/#implicit-query-conversions for details", e.AppendString(nil))
		}
	}

	// Thêm query vào danh sách active queries và thực thi
	qid := activeQueriesV.Add(ec, q)
	rv, err := evalExpr(qt, ec, e)
	activeQueriesV.Remove(qid)
	if err != nil {
		return nil, err
	}

	// Xử lý chế độ isFirstPointOnly - chỉ giữ lại điểm dữ liệu đầu tiên
	if isFirstPointOnly {
		for _, ts := range rv {
			ts.Values = ts.Values[:1]
			ts.Timestamps = ts.Timestamps[:1]
		}
		qt.Printf("leave only the first point in every series")
	}

	// Sắp xếp kết quả nếu cần thiết
	// maySort được xác định dựa trên loại biểu thức, ví dụ các hàm sort() luôn cần sắp xếp
	maySort := maySortResults(e)
	result, err := timeseriesToResult(rv, maySort)

	// Kiểm tra giới hạn số lượng time series trong response
	if *maxResponseSeries > 0 && len(result) > *maxResponseSeries {
		return nil, fmt.Errorf("the response contains more than -search.maxResponseSeries=%d time series: %d series; either increase -search.maxResponseSeries "+
			"or change the query in order to return smaller number of series", *maxResponseSeries, len(result))
	}
	if err != nil {
		return nil, err
	}

	// Log thông tin về việc sắp xếp
	if maySort {
		qt.Printf("sort series by metric name and labels")
	} else {
		qt.Printf("do not sort series by metric name and labels")
	}

	// Làm tròn các giá trị số nếu được yêu cầu
	// RoundDigits < 100 chỉ định số chữ số thập phân cần giữ lại
	if n := ec.RoundDigits; n < 100 {
		for i := range result {
			values := result[i].Values
			for j, v := range values {
				values[j] = decimal.RoundToDecimalDigits(v, n)
			}
		}
		qt.Printf("round series values to %d decimal digits after the point", n)
	}

	return result, nil
}

func maySortResults(e metricsql.Expr) bool {
	switch v := e.(type) {
	case *metricsql.FuncExpr:
		switch strings.ToLower(v.Name) {
		case "sort", "sort_desc", "limit_offset",
			"sort_by_label", "sort_by_label_desc",
			"sort_by_label_numeric", "sort_by_label_numeric_desc":
			// Results already sorted
			return false
		}
	case *metricsql.AggrFuncExpr:
		switch strings.ToLower(v.Name) {
		case "topk", "bottomk", "outliersk",
			"topk_max", "topk_min", "topk_avg", "topk_median", "topk_last",
			"bottomk_max", "bottomk_min", "bottomk_avg", "bottomk_median", "bottomk_last":
			// Results already sorted
			return false
		}
	case *metricsql.BinaryOpExpr:
		if strings.EqualFold(v.Op, "or") {
			// Do not sort results for `a or b` in the same way as Prometheus does.
			// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4763
			return false
		}
	}
	return true
}

func timeseriesToResult(tss []*timeseries, maySort bool) ([]netstorage.Result, error) {
	tss = removeEmptySeries(tss)
	if maySort {
		sortSeriesByMetricName(tss)
	}

	result := make([]netstorage.Result, len(tss))
	m := make(map[string]struct{}, len(tss))
	bb := bbPool.Get()
	for i, ts := range tss {
		bb.B = marshalMetricNameSorted(bb.B[:0], &ts.MetricName)
		k := string(bb.B)
		if _, ok := m[k]; ok {
			return nil, fmt.Errorf(`duplicate output timeseries: %s`, stringMetricName(&ts.MetricName))
		}
		m[k] = struct{}{}

		rs := &result[i]
		rs.MetricName.MoveFrom(&ts.MetricName)
		rs.Values = ts.Values
		ts.Values = nil
		rs.Timestamps = ts.Timestamps
		ts.Timestamps = nil
	}
	bbPool.Put(bb)

	return result, nil
}

func sortSeriesByMetricName(tss []*timeseries) {
	sort.Slice(tss, func(i, j int) bool {
		return metricNameLess(&tss[i].MetricName, &tss[j].MetricName)
	})
}

func metricNameLess(a, b *storage.MetricName) bool {
	if string(a.MetricGroup) != string(b.MetricGroup) {
		return string(a.MetricGroup) < string(b.MetricGroup)
	}
	// Metric names for a and b match. Compare tags.
	// Tags must be already sorted by the caller, so just compare them.
	ats := a.Tags
	bts := b.Tags
	for i := range ats {
		if i >= len(bts) {
			// a contains more tags than b and all the previous tags were identical,
			// so a is considered bigger than b.
			return false
		}
		at := &ats[i]
		bt := &bts[i]
		if string(at.Key) != string(bt.Key) {
			return string(at.Key) < string(bt.Key)
		}
		if string(at.Value) != string(bt.Value) {
			return string(at.Value) < string(bt.Value)
		}
	}
	return len(ats) < len(bts)
}

func removeEmptySeries(tss []*timeseries) []*timeseries {
	rvs := tss[:0]
	for _, ts := range tss {
		allNans := true
		for _, v := range ts.Values {
			if !math.IsNaN(v) {
				allNans = false
				break
			}
		}
		if allNans {
			// Skip timeseries with all NaNs.
			continue
		}
		rvs = append(rvs, ts)
	}
	for i := len(rvs); i < len(tss); i++ {
		// Zero unused time series, so GC could reclaim them.
		tss[i] = nil
	}
	return rvs
}

func adjustCmpOps(e metricsql.Expr) metricsql.Expr {
	metricsql.VisitAll(e, func(expr metricsql.Expr) {
		be, ok := expr.(*metricsql.BinaryOpExpr)
		if !ok {
			return
		}
		if !metricsql.IsBinaryOpCmp(be.Op) {
			return
		}
		if isNumberExpr(be.Right) || !isScalarExpr(be.Left) {
			return
		}
		// Convert 'num cmpOp query' expression to `query reverseCmpOp num` expression
		// like Prometheus does. For instance, `0.5 < foo` must be converted to `foo > 0.5`
		// in order to return valid values for `foo` that are bigger than 0.5.
		be.Right, be.Left = be.Left, be.Right
		be.Op = getReverseCmpOp(be.Op)
	})
	return e
}

func isNumberExpr(e metricsql.Expr) bool {
	_, ok := e.(*metricsql.NumberExpr)
	return ok
}

func isScalarExpr(e metricsql.Expr) bool {
	if isNumberExpr(e) {
		return true
	}
	if fe, ok := e.(*metricsql.FuncExpr); ok {
		// time() returns scalar in PromQL - see https://prometheus.io/docs/prometheus/latest/querying/functions/#time
		return strings.ToLower(fe.Name) == "time"
	}
	return false
}

func getReverseCmpOp(op string) string {
	switch op {
	case ">":
		return "<"
	case "<":
		return ">"
	case ">=":
		return "<="
	case "<=":
		return ">="
	default:
		// there is no need in changing `==` and `!=`.
		return op
	}
}

// parsePromQLWithCache thực hiện parse chuỗi truy vấn PromQL với cơ chế cache.
// Hàm này tối ưu hiệu năng bằng cách:
// 1. Kiểm tra cache cho query string
// 2. Nếu không có trong cache:
//   - Parse query thành AST (Abstract Syntax Tree)
//   - Optimize AST
//   - Điều chỉnh các toán tử so sánh
//   - Xử lý đặc biệt cho dấu chấm trong regex nếu cần
//
// 3. Lưu kết quả vào cache để tái sử dụng
func parsePromQLWithCache(q string) (metricsql.Expr, error) {
	// Thử lấy kết quả parse từ cache
	pcv := parseCacheV.get(q)
	if pcv == nil {
		// Cache miss - thực hiện parse và optimize
		e, err := metricsql.Parse(q)
		if err == nil {
			// Tối ưu hóa AST sau khi parse thành công
			e = metricsql.Optimize(e)
			// Điều chỉnh các toán tử so sánh (==, !=, =~, !~)
			e = adjustCmpOps(e)
			// Xử lý dấu chấm trong regex nếu cần
			if *treatDotsAsIsInRegexps {
				e = escapeDotsInRegexpLabelFilters(e)
			}
		}
		// Lưu kết quả vào cache
		pcv = &parseCacheValue{
			e:   e,
			err: err,
		}
		parseCacheV.put(q, pcv)
	}
	if pcv.err != nil {
		return nil, pcv.err
	}
	return pcv.e, nil
}

func escapeDotsInRegexpLabelFilters(e metricsql.Expr) metricsql.Expr {
	metricsql.VisitAll(e, func(expr metricsql.Expr) {
		me, ok := expr.(*metricsql.MetricExpr)
		if !ok {
			return
		}
		for _, lfs := range me.LabelFilterss {
			for i := range lfs {
				f := &lfs[i]
				if f.IsRegexp {
					f.Value = escapeDots(f.Value)
				}
			}
		}
	})
	return e
}

func escapeDots(s string) string {
	dotsCount := strings.Count(s, ".")
	if dotsCount <= 0 {
		return s
	}
	result := make([]byte, 0, len(s)+2*dotsCount)
	for i := 0; i < len(s); i++ {
		if s[i] == '.' && (i == 0 || s[i-1] != '\\') && (i+1 == len(s) || i+1 < len(s) && s[i+1] != '*' && s[i+1] != '+' && s[i+1] != '{') {
			// Escape a dot if the following conditions are met:
			// - if it isn't escaped already, i.e. if there is no `\` char before the dot.
			// - if there is no regexp modifiers such as '+', '*' or '{' after the dot.
			result = append(result, '\\', '.')
		} else {
			result = append(result, s[i])
		}
	}
	return string(result)
}
