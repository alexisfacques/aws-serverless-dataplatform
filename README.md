# Serverless dataplatform

## Getting started

*TODO*

### What and why?

*TODO*

### Architecture overview

![Architecture overview](./resources/architecture-overview.png)

## Installation

### Software prerequisites

- **AWS CLI** (1.17.14+);
- **Python** (3.8.2+);
  - **boto3** (1.16.8+);
- **AWS SAM CLI** (0.47.0+).

### CloudFormation configuration

#### Required capabilities

- `CAPABILITY_NAMED_IAM`:
  These Cloudformation templates include resources that affect permissions in your AWS account (e.g. creating new AWS Identity and IAM users). You must explicitly acknowledge this by specifying this capability.

- `CAPABILITY_AUTO_EXPAND`:
  Some of these Cloudformation templates contain macros and Cloudformation nested applications. Macros perform custom processing on templates. You must acknowledge this capability.

#### Deploying the application

This assumes your AWS CLI environment is properly configured.

- From the project root directory, run the AWS SAM CLI to build and deploy the Cloudformation application. It is recommended to use the `--guided` option in order to configure the application deployment, including template parameters:

  ```sh
  sam build
  sam deploy --guided --capabilities "CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND"
  ```

- You will be prompt with a selection menu, generating a configuration recap as follows:

  ```sh
  Deploying with following values
  ===============================
  Stack name                   : data-platform
  Region                       : eu-west-1
  Confirm changeset            : True
  Deployment s3 bucket         : <your_cfn_deployment_bucket>
  Capabilities                 : ["CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND"]
  Parameter overrides          : {"EnableEncryption": "True", "KmsKeyArn": "NONE", "EnableApiAuthorization": "True", "ApiAuthorizerArn": "NONE"}
  Signing Profiles             : {}
  ```

- If you opted in confirming the Cloudformation changeset, deploy the application by confirming the changeset.

## What's next?

### 1. Create a CognitoPool user

*TODO*

### 2. Publish a document to the data plaform

*TODO*


### 3. Query your document(s) with Athena

*TODO*
