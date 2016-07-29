![Seneca][SenecaLogo]
> A [Seneca.js][seneca] data storage plugin

# seneca-dynamo-store

seneca-dynamo-store is an [Amazon DynamoDB][dynamodb] database driver for the [Seneca][seneca] MVP toolkit.

## Installation

	npm install --save seneca-dynamo-store

## Usage

    var seneca = require('seneca');
    var senecaDynamoDBStore = require('seneca-dynamo-store');

    var senecaConfig = {}
    var senecaDynamoDBStoreOpts = {
      accessKeyId: 'ACCESSKEYID',
      secretAccessKey: 'SECRETACCESSKEY',
      endpoint: 'ENDPOINT',
      region: 'REGION' // e.g. 'us-east-1'
    };

    ...

    var si = seneca(senecaConfig);
    si.use(senecaDynamoDBStore, senecaDynamoDBStoreOpts);
    si.ready(function() {
      var product = si.make('product');
      ...
    });
    ...

## Configuration

You will need to have an AWS account (obviously!).

### How to get Region and Endpoint

* Go to `https://console.aws.amazon.com/dynamodb/home`
* The URL will be automatically be rewritten to add your Region, e.g. `https://console.aws.amazon.com/dynamodb/home?region=us-east-1`
* Your **Endpoint** will be `https://dynamodb.us-east-1.amazonaws.com` e.g. `https://dynamodb.us-east-1.amazonaws.com`

### How to get Access Keys

* Log into the [AWS IAM Console](https://console.aws.amazon.com/iam/home)
* Press **Users** (on the left)
* Press **Create New Users** (button on the top). This is optional but highly recommended
* Enter a username, e.g. **awsdynamodb_root**. Make sure the "Generate Access Keys" box is selected
* Press **Show User Security Credentials**
* Store the "Access Key" and the "Secret Access Key" in a secure place.
  This is your only opportunity to get the Secret Access Key, so make sure you do this now.

You'll still need to create a Policy and attach it to the User you just created in order to be able to access DynamoDB.

### Create a DynamoDB Root Policy

Still in the AWS IAM Console

* Press **Policies** (on the left)
* Press **Create Policy** (button on the top)
* Press the **Create Your Own Policy** button
* Enter **Policy Name** `DynamoDBRoot` (or whatever)
* Add this **Policy Document**

        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": [
                        "dynamodb:*"
                    ],
                    "Effect": "Allow",
                    "Resource": [
                        "*"
                    ]
                }
            ]
        }

* Press **Create Policy**

### Attach the Policy to the User

* Find the **DynamoDBRoot** policy (Filter is your friend)
* Click on it
* Press tab **Attached Entities**
* Press button **Attach**
* Click radio box for **awsdynamodb_root** (the user we created earlier)
* Press **Attach Policy** button (near bottom)

## Contributing

The [Senecajs org][] encourage open participation. If you feel you can help in any way, be it with documentation, examples, extra testing, or new features please get in touch.

## License

Copyright Seamus D'Arcy and other contributors 2010 - 2016, Licensed under [MIT][].

[comment]: References
[dynamodb]: http://aws.amazon.com/dynamodb
[seneca]: http://senecajs.org/
[SenecaLogo]: http://senecajs.org/files/assets/seneca-logo.png
[Senecajs org]: https://github.com/senecajs/
[MIT]: ./LICENSE.txt
