from google.cloud import storage


def enable_bucket_lifecycle_management(bucket_name):
    """Enable lifecycle management for a bucket"""
    bucket_name = "test-bucket-1173"

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    rules = bucket.lifecycle_rules

    print("Lifecycle management rules for bucket {} are {}".format(bucket_name, list(rules)))
    bucket.add_lifecycle_delete_rule(age=2)
    bucket.patch()

    rules = bucket.lifecycle_rules
    print("Lifecycle management is enable for bucket {} and the rules are {}".format(bucket_name, list(rules)))

    return bucket

if __name__ == '__main__':
    enable_bucket_lifecycle_management('test-bucket-1173')