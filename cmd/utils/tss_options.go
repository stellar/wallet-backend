package utils

import (
	"go/types"

	"github.com/stellar/go/support/config"
)

func RPCCallerServiceChannelBufferSizeOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "tss-rpc-caller-service-channel-buffer-size",
		Usage:       "Set the buffer size for TSS RPC Caller Service channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 1000,
	}
}

func RPCCallerServiceMaxWorkersOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "tss-rpc-caller-service-channel-max-workers",
		Usage:       "Set the maximum number of workers for TSS RPC Caller Service channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 100,
	}

}

func ErrorHandlerServiceJitterChannelBufferSizeOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-service-jitter-channel-buffer-size",
		Usage:       "Set the buffer size of the Error Handler Service Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 100,
		Required:    true,
	}
}

func ErrorHandlerServiceJitterChannelMaxWorkersOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-service-jitter-channel-max-workers",
		Usage:       "Set the maximum number of workers for the Error Handler Service Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    true,
	}
}

func ErrorHandlerServiceNonJitterChannelBufferSizeOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-service-non-jitter-channel-buffer-size",
		Usage:       "Set the buffer size of the Error Handler Service Non Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 100,
		Required:    true,
	}

}

func ErrorHandlerServiceNonJitterChannelMaxWorkersOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-service-non-jitter-channel-max-workers",
		Usage:       "Set the maximum number of workers for the Error Handler Service Non Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    true,
	}
}

func ErrorHandlerServiceJitterChannelMinWaitBtwnRetriesMSOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-service-jitter-channel-min-wait-between-retries",
		Usage:       "Set the minimum amount of time in ms between retries for the Error Handler Service Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    true,
	}
}

func ErrorHandlerServiceNonJitterChannelWaitBtwnRetriesMSOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-service-non-jitter-channel-wait-between-retries",
		Usage:       "Set the amount of time in ms between retries for the Error Handler Service Non Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    true,
	}
}

func ErrorHandlerServiceJitterChannelMaxRetriesOptions(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-service-jitter-channel-max-retries",
		Usage:       "Set the number of retries for each task in the Error Handler Service Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    true,
	}

}

func ErrorHandlerServiceNonJitterChannelMaxRetriesOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-service-non-jitter-channel-max-retries",
		Usage:       "Set the number of retries for each task in the Error Handler Service Non Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    true,
	}
}

func WebhookHandlerServiceChannelMaxBufferSizeOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "webhook-service-channel-max-buffer-size",
		Usage:       "Set the buffer size of the webhook serive channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 100,
		Required:    true,
	}
}

func WebhookHandlerServiceChannelMaxWorkersOptions(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "webhook-service-channel-max-workers",
		Usage:       "Set the max number of workers for the webhook serive channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    true,
	}
}

func WebhookHandlerServiceChannelMaxRetriesOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "webhook-service-channel-max-retries",
		Usage:       "Set the max number of times to ping a webhook before quitting.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 3,
		Required:    true,
	}
}

func WebhookHandlerServiceChannelMinWaitBtwnRetriesMSOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "webhook-service-channel-min-wait-between-retries",
		Usage:       "The minumum amout of time to wait before repining the webhook url",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    true,
	}
}
