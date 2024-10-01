import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';

import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as glue from 'aws-cdk-lib/aws-glue';

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const rawBucket = new s3.Bucket(this, 'rawBucket')
    const processedBucket = new s3.Bucket(this, 'processedBucket')

    const crawlerRole = new iam.Role(this, 'GlueCrawlerRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });

    // Add necessary permissions to the crawler role
    crawlerRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'));
    processedBucket.grantRead(crawlerRole);

    const crawler = new glue.CfnCrawler(this, 'MyGlueCrawler', {
      name: 'raw-data-crawler',
      role: crawlerRole.roleArn,
      databaseName: 'iiot',
      targets: {
        s3Targets: [
          {
            path: `s3://${processedBucket.bucketName}/Hubbox_Sensordata/`
          },
          {
            path: `s3://${processedBucket.bucketName}/Towerbox_Sensordata/`
          },
          {
            path: `s3://${processedBucket.bucketName}/SCADA_Data/`
          },
        ],
      }
    });
    
    const pandasLayer = lambda.LayerVersion.fromLayerVersionArn(this, 'PandasLayer', "arn:aws:lambda:" + this.region + ":336392948345:layer:AWSSDKPandas-Python312:13")

    const func = new lambda.Function(this, 'transformationFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'main.lambda_handler',
      layers: [pandasLayer],
      code: lambda.Code.fromAsset('code/lambda/transformation'), 
      timeout: cdk.Duration.minutes(15),
      environment: {
        "RAW_BUCKET_NAME": rawBucket.bucketName,
        "PROCESSED_BUCKET_NAME": processedBucket.bucketName
      },
      memorySize: 10240
    })
    rawBucket.grantReadWrite(func)
    processedBucket.grantReadWrite(func)

    const successState = new sfn.Pass(this, 'SuccessState', {
      comment: 'Success state',
      result: sfn.Result.fromObject({ message: 'Success' }),
    });

    const errorHandlingState = new sfn.Fail(this, 'ErrorState', {
      comment: 'Handle errors from the Lambda function',
    });

    const processRawFiles = new sfn.DistributedMap(this, "DistributedMap", {
      itemReader: new sfn.S3ObjectsItemReader({
        bucket: rawBucket
      }),
      itemBatcher: new sfn.ItemBatcher({
        maxItemsPerBatch: 50
      })
    })

    processRawFiles.itemProcessor(
      new tasks.LambdaInvoke(this, 'Process', {
        lambdaFunction: func,
        outputPath: "$.Payload"
      })
    )
    .addCatch(errorHandlingState, {
      errors: ['States.ALL'],
      resultPath: '$.error',
    })
    .next(successState)
    .next(new tasks.GlueStartCrawlerRun(this, 'RunCrawler', {
      crawlerName: crawler.ref
    }))
  
    const stateMachine = new sfn.StateMachine(this, 'stateMachine', {
      definitionBody: sfn.DefinitionBody.fromChainable(processRawFiles)
    });
  }
}
