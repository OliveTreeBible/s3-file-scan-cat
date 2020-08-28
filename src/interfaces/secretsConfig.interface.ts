export interface SecretsConfig {
    aws: AWSSecrets
}

export interface AWSSecrets {
    accessKeyId: string
    secretAccessKey: string
}
