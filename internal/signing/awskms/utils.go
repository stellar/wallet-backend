package awskms

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
)

const PrivateKeyEncryptionContext = "pubkey"

func GetPrivateKeyEncryptionContext(publicKey string) map[string]*string {
	return map[string]*string{
		PrivateKeyEncryptionContext: aws.String(publicKey),
	}
}

func GetKMSClient(awsRegion string) (*kms.KMS, error) {
	awsRegion = strings.TrimSpace(awsRegion)
	if awsRegion == "" {
		return nil, fmt.Errorf("aws region cannot be empty")
	}

	sess, err := session.NewSession(&aws.Config{Region: aws.String(awsRegion)})
	if err != nil {
		return nil, fmt.Errorf("getting aws session: %w", err)
	}
	return kms.New(sess), nil
}
