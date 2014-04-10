package model

// iOS cert
type Cert struct {
	ID          int64  `json:"id" bson:"_id"`
	APPID       int64  `json:"app_id" bson:"app_id"`
	DeviceID    int64  `json:"device_id" bson:"device_id"`
	Name        string `json:"name" bson:"name"`
	Description string `json:"description" bson:"description"`
	ReleaseCert string `json:"release_cert" bson:"release_cert"`
	DevCert     string `json:"dev_cert" bson:"dev_cert"`
	CreatedAt   int64  `json:"created_at" bson:"created_at"`
	UpdatedAt   int64  `json:"updated_at" bson:"updated_at"`
}
