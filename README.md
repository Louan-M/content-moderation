# Content moderation

## Description
This is a project for analyzing the content of a video and performing an automatic moderation. The worflow is as follow:

1) input: video to be moderated
2) use of AWS Rekognition service to analyze the content of the video
3) count of the labels to decide if the video is allowed or rejected
4) output: a dictionary with the the labels from Amazon & the label *allowed/rejected* <br /><br />

### AWS Rekognition

To use the API with python, one must install the boto3 library, which is a Python SDK. This can be done via pip:<br />
`pip install boto3`

Regarding the credentials needed to use the API: [See here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)




For the list of all the labels categories: [See here](https://docs.aws.amazon.com/rekognition/latest/dg/moderation.html)


By default, the MinConfidence (prediction threshold) = 50. For this project, the prediction threshold used is **80**.





<br />

## Deployment

Any EC2 instance (ex: *t2.micro*)

Commands:

```
pip3 install boto3
pip3 install pandas
```