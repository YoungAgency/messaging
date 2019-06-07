package pubsub

import (
	"reflect"
	"testing"

	"google.golang.org/api/option"
)

func Test_parseOptions(t *testing.T) {
	type args struct {
		opt *Options
	}
	tests := []struct {
		name    string
		args    args
		wantRet []option.ClientOption
	}{
		// TODO: Add test cases.
		{
			"test preprod",
			args{
				opt: &Options{
					ProjectID: "foo",
				},
			},
			make([]option.ClientOption, 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRet := parseOptions(tt.args.opt); !reflect.DeepEqual(gotRet, tt.wantRet) {
				t.Errorf("parseOptions() = %v, want %v", gotRet, tt.wantRet)
			}
		})
	}
}
