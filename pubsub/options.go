package pubsub

import (
	"fmt"

	"google.golang.org/api/option"
	grpc "google.golang.org/grpc"
)

// Options define how to connect to pub sub
type Options struct {
	Host               string
	Port               int
	ProjectID          string
	SubscriptionName   string
	Token              string
	ServiceAccountPath string
}

func parseOptions(opt *Options) (ret []option.ClientOption) {
	if opt.ServiceAccountPath != "" {
		// local env
		ret = make([]option.ClientOption, 1)
		ret[0] = option.WithCredentialsFile(opt.ServiceAccountPath)
	} else if len(opt.Host) == 0 {
		// preprod
		ret = make([]option.ClientOption, 0)
	} else {
		// old local env
		ret = make([]option.ClientOption, 3)
		ret[0] = option.WithoutAuthentication()
		ret[1] = option.WithEndpoint(fmt.Sprintf("%v:%v", opt.Host, opt.Port))
		ret[2] = option.WithGRPCDialOption(grpc.WithInsecure())
	}
	return
}

// SubscriptionOptions define how to perform a sub on a topic
type SubscriptionOptions struct {
	ConcurrentHandlers int
	SubscriptionName   string
}
