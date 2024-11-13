package utils

import (
	"go/types"

	"github.com/stellar/go/support/config"
)

func RPCCallerChannelBufferSizeOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "tss-rpc-caller-channel-buffer-size",
		Usage:       "Set the buffer size for TSS RPC Caller channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 1000,
	}
}

func RPCCallerChannelMaxWorkersOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "tss-rpc-caller-channel-max-workers",
		Usage:       "Set the maximum number of workers for TSS RPC Caller channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 100,
	}

}

func ErrorHandlerJitterChannelBufferSizeOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-jitter-channel-buffer-size",
		Usage:       "Set the buffer size of the Error Handler Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 1000,
		Required:    true,
	}
}

func ErrorHandlerJitterChannelMaxWorkersOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-jitter-channel-max-workers",
		Usage:       "Set the maximum number of workers for the Error Handler Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 100,
		Required:    true,
	}
}

func ErrorHandlerNonJitterChannelBufferSizeOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-non-jitter-channel-buffer-size",
		Usage:       "Set the buffer size of the Error Handler Non Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 1000,
		Required:    true,
	}

}

func ErrorHandlerNonJitterChannelMaxWorkersOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-non-jitter-channel-max-workers",
		Usage:       "Set the maximum number of workers for the Error Handler Non Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 100,
		Required:    true,
	}
}

func ErrorHandlerJitterChannelMinWaitBtwnRetriesMSOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-jitter-channel-min-wait-between-retries",
		Usage:       "Set the minimum amount of time in ms between retries for the Error Handler Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    true,
	}
}

func ErrorHandlerNonJitterChannelWaitBtwnRetriesMSOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-non-jitter-channel-wait-between-retries",
		Usage:       "Set the amount of time in ms between retries for the Error Handler Non Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    true,
	}
}

func ErrorHandlerJitterChannelMaxRetriesOptions(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-jitter-channel-max-retries",
		Usage:       "Set the number of retries for each task in the Error Handler Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 3,
		Required:    true,
	}

}

func ErrorHandlerNonJitterChannelMaxRetriesOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "error-handler-non-jitter-channel-max-retries",
		Usage:       "Set the number of retries for each task in the Error Handler Service Jitter channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 3,
		Required:    true,
	}
}

func WebhookHandlerChannelMaxBufferSizeOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "webhook-channel-max-buffer-size",
		Usage:       "Set the buffer size of the webhook channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 1000,
		Required:    true,
	}
}

func WebhookHandlerChannelMaxWorkersOptions(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "webhook-channel-max-workers",
		Usage:       "Set the max number of workers for the webhook channel.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 100,
		Required:    true,
	}
}

func WebhookHandlerChannelMaxRetriesOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "webhook-channel-max-retries",
		Usage:       "Set the max number of times to ping a webhook before quitting.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 3,
		Required:    true,
	}
}

func WebhookHandlerChannelMinWaitBtwnRetriesMSOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "webhook-channel-min-wait-between-retries",
		Usage:       "The minumum amout of time to wait before resending the payload to the webhook url",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    true,
	}
}
