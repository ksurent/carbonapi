package filterSeries

import (
	"fmt"
	"math"

	"github.com/bookingcom/carbonapi/expr/helper"
	"github.com/bookingcom/carbonapi/expr/interfaces"
	"github.com/bookingcom/carbonapi/expr/types"
	"github.com/bookingcom/carbonapi/pkg/parser"
)

// XXX(asurikov): graphite-web supports a number of undocumented functions here.
// XXX(asurikov): what does graphite-web return for empty input? Inf vs NaN.
var supportedFuncs = map[string]func([]float64, []bool) float64{
	"max": helper.MaxValue,
	"min": helper.MinValue,
	"sum": helper.MinValue,
	"avg": helper.AvgValue,
	// "multiply": ,
	// "diff":     ,
	// "median":   ,
	// "stddev":   ,
	// "last":     ,
	// "range":    ,
}

// XXX(asurikov): does float comparison in Go work the same way as in Python?
var supportedOps = map[string]func(float64, float64) bool{
	"<":  func(a, b float64) bool { return !math.IsNaN(a) && a < b },
	">":  func(a, b float64) bool { return !math.IsNaN(a) && a > b },
	"<=": func(a, b float64) bool { return !math.IsNaN(a) && a <= b },
	">=": func(a, b float64) bool { return !math.IsNaN(a) && a >= b },
	"=":  func(a, b float64) bool { return !math.IsNaN(a) && a == b },
	"!=": func(a, b float64) bool { return !math.IsNaN(a) && a != b },
}

type filterSeries struct {
	interfaces.FunctionBase
}

// filterSeries(system.interface.eth*.packetsSent, 'max', '>', 1000)
func (f *filterSeries) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		return nil, err
	}

	fname, err := e.GetStringArg(1)
	if err != nil {
		return nil, err
	}

	opname, err := e.GetStringArg(2)
	if err != nil {
		return nil, err
	}

	threshold, err := e.GetFloatArg(3)
	if err != nil {
		return nil, err
	}

	aggf, ok := supportedFuncs[fname]
	if !ok {
		return nil, fmt.Errorf("unsupported consolidation function: %q", fname)
	}

	cmpf, ok := supportedOps[opname]
	if !ok {
		return nil, fmt.Errorf("unsupported operator: %q", opname)
	}

	filtered := make([]*types.MetricData, 0, len(args))
	for _, a := range args {
		// The second argument to cmpf must not be a NaN.
		if n := aggf(a.Values, a.IsAbsent); cmpf(n, threshold) {
			filtered = append(filtered, a)
		}
	}

	return filtered, nil
}

func Description() map[string]types.FunctionDescription {
	return nil
}
