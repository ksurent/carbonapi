package summarize

import (
	"fmt"
	"github.com/go-graphite/carbonapi/expr/helper"
	"github.com/go-graphite/carbonapi/expr/interfaces"
	"github.com/go-graphite/carbonapi/expr/metadata"
	"github.com/go-graphite/carbonapi/expr/types"
	"github.com/go-graphite/carbonapi/pkg/parser"
	pb "github.com/go-graphite/carbonzipper/carbonzipperpb3"
	"math"
)

func init() {
	f := &summarize{}
	functions := []string{"summarize"}
	for _, function := range functions {
		metadata.RegisterFunction(function, f)
	}
}

type summarize struct {
	interfaces.FunctionBase
}

// summarize(seriesList, intervalString, func='sum', alignToFrom=False)
func (f *summarize) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	// TODO(dgryski): make sure the arrays are all the same 'size'
	args, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return nil, nil
	}

	bucketSize, err := e.GetIntervalArg(1, 1)
	if err != nil {
		return nil, err
	}

	summarizeFunction, err := e.GetStringNamedOrPosArgDefault("func", 2, "sum")
	if err != nil {
		return nil, err
	}
	_, funcOk := e.NamedArgs()["func"]
	if !funcOk {
		funcOk = len(e.Args()) > 2
	}

	alignToFrom, err := e.GetBoolNamedOrPosArgDefault("alignToFrom", 3, false)
	if err != nil {
		return nil, err
	}
	_, alignOk := e.NamedArgs()["alignToFrom"]
	if !alignOk {
		alignOk = len(e.Args()) > 3
	}

	start := args[0].StartTime
	stop := args[0].StopTime
	if !alignToFrom {
		start, stop = helper.AlignToBucketSize(start, stop, bucketSize)
	}

	buckets := helper.GetBuckets(start, stop, bucketSize)
	results := make([]*types.MetricData, 0, len(args))
	for _, arg := range args {

		name := fmt.Sprintf("summarize(%s,'%s'", arg.Name, e.Args()[1].StringValue())
		if funcOk || alignOk {
			// we include the "func" argument in the presence of
			// "alignToFrom", even if the former was omitted
			// this is so that a call like "summarize(foo, '5min', alignToFrom=true)"
			// doesn't produce a metric name that has a boolean value
			// where a function name should be
			// so we show "summarize(foo,'5min','sum',true)" instead of "summarize(foo,'5min',true)"
			//
			// this does not match graphite's behaviour but seems more correct
			name += fmt.Sprintf(",'%s'", summarizeFunction)
		}
		if alignOk {
			name += fmt.Sprintf(",%v", alignToFrom)
		}
		name += ")"

		if arg.StepTime > bucketSize {
			// We don't have enough data to do math
			results = append(results, &types.MetricData{FetchResponse: pb.FetchResponse{
				Name:      name,
				Values:    arg.Values,
				IsAbsent:  arg.IsAbsent,
				StepTime:  arg.StepTime,
				StartTime: arg.StartTime,
				StopTime:  arg.StopTime,
			}})
			continue
		}

		r := types.MetricData{FetchResponse: pb.FetchResponse{
			Name:      name,
			Values:    make([]float64, buckets, buckets),
			IsAbsent:  make([]bool, buckets, buckets),
			StepTime:  bucketSize,
			StartTime: start,
			StopTime:  stop,
		}}

		t := arg.StartTime // unadjusted
		bucketEnd := start + bucketSize
		values := make([]float64, 0, bucketSize/arg.StepTime)
		ridx := 0
		bucketItems := 0
		for i, v := range arg.Values {
			bucketItems++
			if !arg.IsAbsent[i] {
				values = append(values, v)
			}

			t += arg.StepTime

			if t >= stop {
				break
			}

			if t >= bucketEnd {
				rv := helper.SummarizeValues(summarizeFunction, values)

				if math.IsNaN(rv) {
					r.IsAbsent[ridx] = true
				}

				r.Values[ridx] = rv
				ridx++
				bucketEnd += bucketSize
				bucketItems = 0
				values = values[:0]
			}
		}

		// last partial bucket
		if bucketItems > 0 {
			rv := helper.SummarizeValues(summarizeFunction, values)
			if math.IsNaN(rv) {
				r.Values[ridx] = 0
				r.IsAbsent[ridx] = true
			} else {
				r.Values[ridx] = rv
				r.IsAbsent[ridx] = false
			}
		}

		results = append(results, &r)
	}
	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
func (f *summarize) Description() map[string]*types.FunctionDescription {
	return map[string]*types.FunctionDescription{
		"summarize": {
			Description: "Summarize the data into interval buckets of a certain size.\n\nBy default, the contents of each interval bucket are summed together. This is\nuseful for counters where each increment represents a discrete event and\nretrieving a \"per X\" value requires summing all the events in that interval.\n\nSpecifying 'average' instead will return the mean for each bucket, which can be more\nuseful when the value is a gauge that represents a certain value in time.\n\nThis function can be used with aggregation functions ``average``, ``median``, ``sum``, ``min``,\n``max``, ``diff``, ``stddev``, ``count``, ``range``, ``multiply`` & ``last``.\n\nBy default, buckets are calculated by rounding to the nearest interval. This\nworks well for intervals smaller than a day. For example, 22:32 will end up\nin the bucket 22:00-23:00 when the interval=1hour.\n\nPassing alignToFrom=true will instead create buckets starting at the from\ntime. In this case, the bucket for 22:32 depends on the from time. If\nfrom=6:30 then the 1hour bucket for 22:32 is 22:30-23:30.\n\nExample:\n\n.. code-block:: none\n\n  &target=summarize(counter.errors, \"1hour\") # total errors per hour\n  &target=summarize(nonNegativeDerivative(gauge.num_users), \"1week\") # new users per week\n  &target=summarize(queue.size, \"1hour\", \"avg\") # average queue size per hour\n  &target=summarize(queue.size, \"1hour\", \"max\") # maximum queue size during each hour\n  &target=summarize(metric, \"13week\", \"avg\", true)&from=midnight+20100101 # 2010 Q1-4",
			Function:    "summarize(seriesList, intervalString, func='sum', alignToFrom=False)",
			Group:       "Transform",
			Module:      "graphite.render.functions",
			Name:        "summarize",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "intervalString",
					Required: true,
					Suggestions: []string{
						"10min",
						"1h",
						"1d",
					},
					Type: types.Interval,
				},
				{
					Default: "sum",
					Name:    "func",
					Options: []string{
						"average",
						"count",
						"diff",
						"last",
						"max",
						"median",
						"min",
						"multiply",
						"range",
						"stddev",
						"sum",
					},
					Type: types.AggFunc,
				},
				{
					Default: "false",
					Name:    "alignToFrom",
					Type:    types.Boolean,
				},
			},
		},
	}
}