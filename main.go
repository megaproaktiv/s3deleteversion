package main

import (
    "context"
    "fmt"
    "log"
    "os"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3Client struct {
    *s3.Client
}

func main() {
 
    bucketName := os.Getenv("S3_BUCKET")

    cfg, err := config.LoadDefaultConfig(context.TODO())
    if err != nil {
        log.Fatalf("Cannot load the AWS configs: %s", err)
    }

    serviceClient := s3.NewFromConfig(cfg)

    client := &s3Client{
        Client: serviceClient,
    }

    fmt.Printf(">>> Bucket from S3_BUCKET: %s\n", bucketName)

    objects, err := client.listObjects(bucketName)
    if err != nil {
        log.Fatal(err)
    }
    if len(objects) > 0 {
        fmt.Printf(">>> List objects in the bucket: \n")
        for _, object := range objects {
            fmt.Printf("%s\n", object)
        }
    } else {
        fmt.Printf(">>> No objects in the bucket.\n")
    }

    if client.versioningEnabled(bucketName) {
        fmt.Printf(">>> Versioning is enabled.\n")
        objectVersions, err := client.listObjectVersions(bucketName)
        if err != nil {
            log.Fatal(err)
        }
        if len(objectVersions) > 0 {
            fmt.Printf(">>> List objects with versions: \n")
            for key, versions := range objectVersions {
                fmt.Printf("%s: ", key)
                for _, version := range versions {
                    fmt.Printf("\n\t%s ", version)
                }
                fmt.Println()
            }
        }

        if len(objectVersions) > 0 {
            fmt.Printf(">>> Delete objects with versions.\n")
            if err := client.deleteObjects(bucketName, objectVersions); err != nil {
                log.Fatal(err)
            }

            objectVersions, err = client.listObjectVersions(bucketName)
            if err != nil {
                log.Fatal(err)
            }
            if len(objectVersions) > 0 {
                fmt.Printf(">>> List objects with versions after deletion: \n")
                for key, version := range objectVersions {
                    fmt.Printf("%s: %s\n", key, version)
                }
            } else {
                fmt.Printf(">>> No objects in the bucket after deletion.\n")
            }
        }
    }

    fmt.Printf(">>> Delete the bucket.\n")
    if err := client.deleteBucket(bucketName); err != nil {
        log.Fatal(err)
    }

}

func (c *s3Client) versioningEnabled(bucket string) bool {
    output, err := c.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
        Bucket: aws.String(bucket),
    })
    if err != nil {
        return false
    }
    return output.Status == "Enabled"
}

func (c *s3Client) listObjects(bucket string) ([]string, error) {
    var objects []string
    output, err := c.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
        Bucket: aws.String(bucket),
    })
    if err != nil {
        return nil, err
    }

    for _, object := range output.Contents {
        objects = append(objects, aws.ToString(object.Key))
    }

    return objects, nil
}

func (c *s3Client) listObjectVersions(bucket string) (map[string][]string, error) {
    var objectVersions = make(map[string][]string)
    output, err := c.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
        Bucket: aws.String(bucket),
    })
    if err != nil {
        return nil, err
    }

    for _, object := range output.Versions {
        fmt.Printf(">>> objects Verson %v \n", object.Key)

        if _, ok := objectVersions[aws.ToString(object.Key)]; ok {
            objectVersions[aws.ToString(object.Key)] = append(objectVersions[aws.ToString(object.Key)], aws.ToString(object.VersionId))
        } else {
            objectVersions[aws.ToString(object.Key)] = []string{aws.ToString(object.VersionId)}
        }
    }

    for _, object := range output.DeleteMarkers {
        fmt.Printf(">>> objects Delete Marker %v \n", object.Key)

        if _, ok := objectVersions[aws.ToString(object.Key)]; ok {
            objectVersions[aws.ToString(object.Key)] = append(objectVersions[aws.ToString(object.Key)], aws.ToString(object.VersionId))
        } else {
            objectVersions[aws.ToString(object.Key)] = []string{aws.ToString(object.VersionId)}
        }
    }

    return objectVersions, err
}

func (c *s3Client) deleteObjects(bucket string, objectVersions map[string][]string) error {
    var identifiers []types.ObjectIdentifier
    for key, versions := range objectVersions {
        for _, version := range versions {
            identifiers = append(identifiers, types.ObjectIdentifier{
                Key:       aws.String(key),
                VersionId: aws.String(version),
            })
        }
    }

    _, err := c.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
        Bucket: aws.String(bucket),
        Delete: &types.Delete{
            Objects: identifiers,
        },
    })
    if err != nil {
        return err
    }
    return nil
}

func (c *s3Client) deleteBucket(bucket string) error {
    _, err := c.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
        Bucket: aws.String(bucket),
    })
    if err != nil {
        return err
    }

    return nil
}
