
def content_moderation(video_path:str, min_confidence=80.0, poll_wait_time=10) -> dict:
    """
    Purpose

    Perform content moderation on commercial videos by use of AWS Rekognition API.

    :attrib path will specify the path to the video
    :attrib min_confidence will set the minimum detection threshold of the model
    :attrib poll_wait_time will specify the waiting time between each notification poll
    :return: A dictionnary with the label count for each category from AWS Rekognition service and the prediction (allowed || rejected) 
    """

    import logging
    import json
    import os
    import time
    import boto3
    from botocore.exceptions import ClientError
    import pandas as pd

    # configure aws SDK
    os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'
    os.environ['AWS_ACCESS_KEY_ID'] = ''
    os.environ['AWS_SECRET_ACCESS_KEY'] = ''


    logger = logging.getLogger(__name__)


    class RekognitionVideo:
        """
        Encapsulates an Amazon Rekognition video. This class is a thin wrapper around
        parts of the Boto3 Amazon Rekognition API.
        """
        def __init__(self, video, video_name, rekognition_client):
            """
            Initializes the video object.

            :param video: Amazon S3 bucket and object key data where the video is located.
            :param video_name: The name of the video.
            :param rekognition_client: A Boto3 Rekognition client.
            """
            self.video = video
            self.video_name = video_name
            self.rekognition_client = rekognition_client
            self.topic = None
            self.queue = None
            self.role = None

        @classmethod
        def from_bucket(cls, s3_object, rekognition_client):
            """
            Creates a RekognitionVideo object from an Amazon S3 object.

            :param s3_object: An Amazon S3 object that contains the video. The video
                            is not retrieved until needed for a later call.
            :param rekognition_client: A Boto3 Rekognition client.
            :return: The RekognitionVideo object, initialized with Amazon S3 object data.
            """
            video = {'S3Object': {'Bucket': s3_object.bucket_name, 'Name': s3_object.key}}
            return cls(video, s3_object.key, rekognition_client)

        def create_notification_channel(
                self, resource_name, iam_resource, sns_resource, sqs_resource):
            """
            Creates a notification channel used by Amazon Rekognition to notify subscribers
            that a detection job has completed. The notification channel consists of an
            Amazon SNS topic and an Amazon SQS queue that is subscribed to the topic.

            After a job is started, the queue is polled for a job completion message.
            Amazon Rekognition publishes a message to the topic when a job completes,
            which triggers Amazon SNS to send a message to the subscribing queue.

            As part of creating the notification channel, an AWS Identity and Access
            Management (IAM) role and policy are also created. This role allows Amazon
            Rekognition to publish to the topic.

            :param resource_name: The name to give to the channel resources that are
                                created.
            :param iam_resource: A Boto3 IAM resource.
            :param sns_resource: A Boto3 SNS resource.
            :param sqs_resource: A Boto3 SQS resource.
            """
            self.topic = sns_resource.create_topic(Name=resource_name)
            self.queue = sqs_resource.create_queue(
                QueueName=resource_name, Attributes={'ReceiveMessageWaitTimeSeconds': '5'})
            queue_arn = self.queue.attributes['QueueArn']

            # This policy lets the queue receive messages from the topic.
            self.queue.set_attributes(Attributes={'Policy': json.dumps({
                'Version': '2008-10-17',
                'Statement': [{
                    'Sid': 'test-sid',
                    'Effect': 'Allow',
                    'Principal': {'AWS': '*'},
                    'Action': 'SQS:SendMessage',
                    'Resource': queue_arn,
                    'Condition': {'ArnEquals': {'aws:SourceArn': self.topic.arn}}}]})})
            self.topic.subscribe(Protocol='sqs', Endpoint=queue_arn)

            # This role lets Amazon Rekognition publish to the topic. Its Amazon Resource
            # Name (ARN) is sent each time a job is started.
            self.role = iam_resource.create_role(
                RoleName=resource_name,
                AssumeRolePolicyDocument=json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': {'Service': 'rekognition.amazonaws.com'},
                            'Action': 'sts:AssumeRole'
                        }
                    ]
                })
            )
            policy = iam_resource.create_policy(
                PolicyName=resource_name,
                PolicyDocument=json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': 'SNS:Publish',
                            'Resource': self.topic.arn
                        }
                    ]
                })
            )
            self.role.attach_policy(PolicyArn=policy.arn)

        def get_notification_channel(self):
            """
            Gets the role and topic ARNs that define the notification channel.

            :return: The notification channel data.
            """
            return {'RoleArn': self.role.arn, 'SNSTopicArn': self.topic.arn}

        def delete_notification_channel(self):
            """
            Deletes all of the resources created for the notification channel.
            """
            for policy in self.role.attached_policies.all():
                self.role.detach_policy(PolicyArn=policy.arn)
                policy.delete()
            self.role.delete()
            logger.info("Deleted role %s.", self.role.role_name)
            self.role = None
            self.queue.delete()
            logger.info("Deleted queue %s.", self.queue.url)
            self.queue = None
            self.topic.delete()
            logger.info("Deleted topic %s.", self.topic.arn)
            self.topic = None

        def poll_notification(self, job_id):
            """
            Polls the notification queue for messages that indicate a job has completed.

            :param job_id: The ID of the job to wait for.
            :return: The completion status of the job.
            """
            status = None
            job_done = False
            while not job_done:
                messages = self.queue.receive_messages(
                    MaxNumberOfMessages=1, WaitTimeSeconds=poll_wait_time)
                logger.info("Polled queue for messages, got %s.", len(messages))
                if messages:
                    body = json.loads(messages[0].body)
                    message = json.loads(body['Message'])
                    if job_id != message['JobId']:
                        raise RuntimeError
                    status = message['Status']
                    logger.info("Got message %s with status %s.", message['JobId'], status)
                    messages[0].delete()
                    job_done = True
            return status

        def _start_rekognition_job(self, job_description, start_job_func):
            """
            Starts a job by calling the specified job function.

            :param job_description: A description to log about the job.
            :param start_job_func: The specific Boto3 Rekognition start job function to
                                call, such as start_label_detection.
            :return: The ID of the job.
            """
            try:
                response = start_job_func(
                    Video=self.video, NotificationChannel=self.get_notification_channel(), MinConfidence=min_confidence)
                job_id = response['JobId']
                logger.info(
                    "Started %s job %s on %s.", job_description, job_id, self.video_name)
            except ClientError:
                logger.exception(
                    "Couldn't start %s job on %s.", job_description, self.video_name)
                raise
            else:
                return job_id

        def _get_rekognition_job_results(self, job_id, get_results_func):
            """
            Gets the results of a completed job by calling the specified results function.
            Results are extracted into objects by using the specified extractor function.

            :param job_id: The ID of the job.
            :param get_results_func: The specific Boto3 Rekognition get job results
                                    function to call, such as get_label_detection.
            :param result_extractor: A function that takes the results of the job
                                    and wraps the result data in object form.
            :return: The list of result objects.
            """
            try:
                response = get_results_func(JobId=job_id)
                logger.info("Job %s has status: %s.", job_id, response['JobStatus'])
                
                
            except ClientError:
                logger.exception("Couldn't get items for %s.", job_id)
                raise
            else:
                return response

        def _do_rekognition_job(
                self, job_description, start_job_func, get_results_func):
            """
            Starts a job, waits for completion, and gets the results.

            :param job_description: The description of the job.
            :param start_job_func: The Boto3 start job function to call.
            :param get_results_func: The Boto3 get job results function to call.
            :param result_extractor: A function that can extract the results into objects.
            :return: The list of result objects.
            """
            job_id = self._start_rekognition_job(job_description, start_job_func)
            status = self.poll_notification(job_id)
            if status == 'SUCCEEDED':
                results = self._get_rekognition_job_results(
                    job_id, get_results_func)
                
            else:
                results = []
            return results

    
        def do_content_moderation(self):
            """
            Performs content moderation on the video.

            :return: The list of moderation labels found in the video.
            """
            return self._do_rekognition_job(
                "content moderation",
                self.rekognition_client.start_content_moderation,
                self.rekognition_client.get_content_moderation,
    )


 
    print('-'*88)
    print("Starting content moderation")
    print('-'*88)

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    print("Creating Amazon S3 bucket and uploading video.")
    s3_resource = boto3.resource('s3')

    bucket = s3_resource.create_bucket(
        Bucket=f'bucket-rekognition-{time.time_ns()}',
        CreateBucketConfiguration={
            'LocationConstraint': s3_resource.meta.client.meta.region_name
        })
    video_object = bucket.Object(video_path)
    vid = video_path
    video_object.upload_file(vid)

    rekognition_client = boto3.client('rekognition')
    video = RekognitionVideo.from_bucket(video_object, rekognition_client)

    print("Creating notification channel from Amazon Rekognition to Amazon SQS.")
    iam_resource = boto3.resource('iam')
    sns_resource = boto3.resource('sns')
    sqs_resource = boto3.resource('sqs')
    video.create_notification_channel(
        'test-video-rekognition', iam_resource, sns_resource, sqs_resource)

    print("Detecting labels in the video.")
    get_labels = video.do_content_moderation()

        
    print("Deleting resources created for the video.")
    video.delete_notification_channel()
    bucket.objects.delete()
    bucket.delete()
    logger.info("Deleted bucket %s.", bucket.name)
    print("All resources cleaned up!")
    print('-'*88)

    df = pd.DataFrame(columns=["nudity", "graphic_male_nudity", "graphic_female_nudity", "sexual_activity",
    "illustrated_explicit_nudity", "adult_toys", "female_swimwear_or_underwear", "male_swimwear_or_underwear",
    "partial_nudity", "barechested_male", "revealing_clothes", "sexual_situations", "graphic_violence_or_gore",
    "physical_violence", "weapon_violence", "weapons", "self_injury", "emaciated_bodies", "corpses", "hanging",
    "air_crash", "explosions_or_blasts", "middle_finger", "drug_products", "drug_use", "pills", "drug_paraphernalia",
    "tobacco_products", "smoking", "drinking", "alcoholic_beverages", "gambling", "nazi_party", "white_supremacy",
    "extremist"])

    # Parent category: Nudity
    nudity = 0
    graphic_male_nudity = 0
    graphic_female_nudity = 0
    sexual_activity = 0
    illustrated_explicit_nudity = 0
    adult_toys = 0

    # Parent category: Suggestive
    female_swimwear_or_underwear = 0
    male_swimwear_or_underwear = 0
    partial_nudity = 0
    barechested_male = 0
    revealing_clothes = 0
    sexual_situations = 0

    # Parent category: Violence
    graphic_violence_or_gore = 0
    physical_violence = 0
    weapon_violence = 0
    weapons = 0
    self_injury = 0

    # Parent category: Visually disturbing
    emaciated_bodies = 0
    corpses = 0
    hanging = 0
    air_crash = 0
    explosions_or_blasts = 0

    # Parent category: Rude Gestures
    middle_finger = 0

    # Parent category: Drugs
    drug_products = 0
    drug_use = 0
    pills = 0
    drug_paraphernalia = 0

    # Parent category: Tobacco
    tobacco_products = 0
    smoking = 0

    # Parent category: Alcohol
    drinking = 0
    alcoholic_beverages = 0

    # Parent category: Gambling
    gambling = 0

    # Parent category: Hate Symbols
    nazi_party = 0
    white_supremacy = 0
    extremist = 0

    for labels in get_labels['ModerationLabels']:
        label = labels['ModerationLabel']['Name']

        # Nudity
        if label == "Nudity":
            nudity +=1

        elif label == "Graphic Male Nudity":
            graphic_male_nudity +=1

        elif label == "Graphic Female Nudity":
            graphic_female_nudity +=1

        elif label == "Sexual Activity":
            sexual_activity +=1

        elif label == "Illustrated Explicity Nudity":
            illustrated_explicit_nudity +=1

        elif label == "Adult Toys":
            adult_toys +=1


        # Suggestive
        elif label == "Female Swimwear Or Underwear":
            female_swimwear_or_underwear +=1

        elif label == "Male Swimwear Or Underwear":
            male_swimwear_or_underwear +=1

        elif label == "Partial Nudity":
            partial_nudity +=1

        elif label == "Barechested Male":
            barechested_male +=1

        elif label == "Revealing Clothes":
            revealing_clothes +=1

        elif label == "Sexual Situations":
            sexual_situations +=1


        # Violence
        elif label == "Graphic Violence Or Gore":
            graphic_violence_or_gore +=1

        elif label == "Physical Violence":
            physical_violence +=1

        elif label == "Weapon Violence":
            weapon_violence +=1

        elif label == "Weapons":
            weapons +=1

        elif label == "Self Injury":
            self_injury +=1


        # Visually disturbing
        elif label == "Emaciated Bodies":
            emaciated_bodies +=1

        elif label == "Corpses":
            corpses +=1

        elif label == "Hanging":
            hanging +=1

        elif label == "Air Crash":
            air_crash +=1

        elif label == "Explosion Or Blasts":
            explosions_or_blasts +=1


        # Rude Gestures
        elif label == "Middle Finger":
            middle_finger +=1


        # Drugs
        elif label == "Drug Products":
            drug_products +=1

        elif label == "Drug Use":
            drug_use +=1

        elif label == "Pills":
            pills +=1

        elif label == "Drug Paraphernalia":
            drug_paraphernalia +=1

        elif label == "Tobacco Products":
            tobacco_products +=1

        elif label == "Smoking":
            smoking +=1


        # Alcohol
        elif label == "Drinking":
            drinking +=1


        # Gambling
        elif label == "Gambling":
            gambling +=1
        

        # Hate Symbols
        elif label == "Nazi Party":
            nazi_party +=1

        elif label == "White Supremacy":
            white_supremacy +=1
        
        elif label == "Extremist":
            extremist +=1
        

    new_row = {

    "nudity": nudity, "graphic_male_nudity": graphic_male_nudity, "graphic_female_nudity": graphic_female_nudity,
    "sexual_activity": sexual_activity, "illustrated_explicit_nudity": illustrated_explicit_nudity, "adult_toys": adult_toys,
    "female_swimwear_or_underwear": female_swimwear_or_underwear, "male_swimwear_or_underwear": male_swimwear_or_underwear,
    "partial_nudity": partial_nudity, "barechested_male": barechested_male, "revealing_clothes": revealing_clothes,
    "sexual_situations": sexual_situations, "graphic_violence_or_gore": graphic_violence_or_gore,
    "physical_violence": physical_violence, "weapon_violence": weapon_violence, "weapons": weapons, "self_injury": self_injury,
    "emaciated_bodies": emaciated_bodies, "corpses": corpses, "hanging": hanging, "air_crash": air_crash,
    "explosions_or_blasts": explosions_or_blasts, "middle_finger": middle_finger, "drug_products": drug_products,
    "drug_use": drug_use, "pills": pills, "drug_paraphernalia":drug_paraphernalia, "tobacco_products": tobacco_products,
    "smoking": smoking, "drinking":drinking, "alcoholic_beverages": alcoholic_beverages, "gambling": gambling,
    "nazi_party": nazi_party, "white_supremacy": white_supremacy, "extremist": extremist
    }

    df = df.append(new_row, ignore_index=True, sort=None)
    
    

    labels_count = df.iloc[0,:].sum()

    if labels_count < 1:
        df['label'] = 'allowed'
    else:
        df['label'] = 'rejected'

    pred_values = df.iloc[0,:].to_dict()

    return pred_values



if __name__ == '__main__':
    prediction = content_moderation('total-engag-pour-une-nergie-meilleure-fr.mp4')
    print(prediction)
