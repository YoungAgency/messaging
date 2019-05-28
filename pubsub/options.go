package pubsub

import (
	"fmt"
	"os"

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
	// local env
	if opt.ProjectID == "" {
		s := os.Getenv("PROJECTID")
		if s == "" {
			panic("Invalid project id")
		}
		opt.ProjectID = s
		return make([]option.ClientOption, 0)
	}
	if opt.ServiceAccountPath != "" {
		ret = make([]option.ClientOption, 1)
		ret[0] = option.WithCredentialsFile(opt.ServiceAccountPath)
		return
	}
	// preprod
	if len(opt.Host) == 0 && opt.Port == 0 {
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
