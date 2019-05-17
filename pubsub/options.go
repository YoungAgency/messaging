package pubsub

import (
	"fmt"

	"google.golang.org/api/option"
	grpc "google.golang.org/grpc"
)

type Options struct {
	Host             string
	Port             int
	ProjectID        string
	SubscriptionName string
	Token            string
}

func parseOptions(opt *Options) (ret []option.ClientOption) {
	if len(opt.Host) == 0 {
		ret = make([]option.ClientOption, 0)
	} else {
		ret = make([]option.ClientOption, 3)
		ret[0] = option.WithoutAuthentication()
		ret[1] = option.WithEndpoint(fmt.Sprintf("%v:%v", opt.Host, opt.Port))
		ret[2] = option.WithGRPCDialOption(grpc.WithInsecure())
	}
	return
}

type SubscriptionOptions struct {
	ConcurrentHandlers int
}
