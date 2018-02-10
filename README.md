# Apache Beam Workshop
This is a full course that should help you immerse yourself in Apache Beam.
Through this workshop, we will review Batch and Streaming concepts, and apply
them to build some data analytics pipelines.

Also check out [the slides for the course](https://docs.google.com/presentation/d/1-6YSMrMUQqXJzFKKzt_M4_0REIGucRkVHBvShyI1pQo/edit#slide=id.g2b4f1c9a00_1_3316).

For this, you will need the following:
* A Github account, to clone this repository
* A setup for some Beam runner:
  * A Spark Cluster [Installer: https://archive.apache.org/dist/spark/spark-1.6.0/spark-1.6.0-bin-hadoop2.6.tgz]
  * A Flink Cluster
  * A GCP Project with Dataflow enabled
  * The Beam Direct runner
* It's desirable, but not necessary, to have familiarity with the high-level
  concepts of distributed data processing.
  
  
### Software Requirements
You will need to have Maven 3.3.1+, and JDK 7+ installed. To install them:

* OpenJDK http://openjdk.java.net/install/
* Maven [Download: https://maven.apache.org/download.cgi, Install: https://maven.apache.org/install.html]
* Git   https://git-scm.com/book/en/v2/Getting-Started-Installing-Git
* [Optional] Google Cloud Command Line Interface https://cloud.google.com/sdk/downloads/ 

### Java / Python SDKs and the different Runners
This workshop is mainly geared for Java, and the different runners that support
the Beam Java SDK.

Python examples are also provided, though these only work on the 
Direct Runner and the Dataflow Runner. Check out the `py/` directory
and its `README` for the Python examples.

## Getting started
First of all, you'll need to set up the basic environment variables that you
will use to run the pipelines in different runners. Run:

    source setup.sh

Normally the default values will be enough, but check with the instructor to make
sure that you get all the proper variables. Also, you will have the option to have the
Python environment set up, and the Google Cloud SDK to access Google Cloud Storage.

### Checking the results of your pipelines
If you use the provided `$GCP_OUTPUT_FILE` to output the results of your pipeline, you 
should be able to use `gsutil ls $GCP_OUTPUT_FILE` to see the results of your jobs, as
well as other file commands.

## The GettingStarted pipeline
This file is for you to iterate over your solutions. You can use the Maven commands
that we will use for other pipelines, except you'll just need to substitute the
class to `-Dexec.mainClass="demo.GettingStarted"`.

## The UserScore pipeline
We have coded a mobile game, and it's become successful! We have millions of users around the world 
that are playing it multiple times per day. Whenever a user plays our game, they perform some
silly repetitive action, and score points. 

After every game, their score gets reported back to our data infrastructure, along with the
user name, team name, and a time stamp for when the user played that game.

We want to analyze this data. Initially, we'd like to create a pipeline that sums all the 
points obtained by every user individually; given that we are storing comma-separated
strings with their user name, team name, score, and timestamp.

Look at the code in `src/main/java/demo/UserScore.java`.

    mvn clean package exec:java -Pdirect-runner \
        -Dexec.mainClass="demo.UserScore" \
        -Dexec.args="--runner=DirectRunner --input=$BEAM_LOCAL_FILE --outputPrefix=data/outputfiles"
        
    mvn clean package exec:java -Pdataflow-runner \
        -Dexec.mainClass="demo.UserScore" \
        -Dexec.args="--runner=DataflowRunner \
                     --input=$GCP_INPUT_FILE \
                     --outputPrefix=$GCP_OUTPUT_FILE/dataflow/user/res \
                     --project=$GCP_PROJECT"
                     
    # Submit job to your spark cluster in GCP.
    mvn clean package -Pspark-runner
    gcloud dataproc jobs submit spark \
            --project=$GCP_PROJECT \
            --cluster gaming-spark \
            --properties spark.default.parallelism=200 \
            --class demo.UserScore \
            --jars ./target/portability-demo-bundled-spark.jar \
            -- \
            --runner=spark \
            --input=$GCP_INPUT_FILE \
            --outputPrefix=$GCP_OUTPUT_FILE/spark/user/res

To submit your pipeline to Flink, you will need to go into the Flink UI (provided by your instructor).
Once there, you can build the JAR for Flink (`mvn clean package -Pflink-runner`), upload it through the Flink UI, and select class 
`demo.UserScore` and pass the following arguments:

    --parallelism=20 --input=gs://apache-beam-demo/data/gaming* 
    --outputPrefix=gs://beam-workshop-outputs/yourusername/flink/user/scores
    --runner=flink
    
## The HourlyTeamScore pipeline
Now, suppose that we want to step up our analytics. Instead of just adding up global scores,
suppose that we want to study how much each team performed every hour instead. This means that
we have to divide our data over two dimensions: by their timestamps, and by their team name.

Look at the code in `src/main/java/demo/HourlyTeamScore.java`.


    mvn clean package exec:java -Pdirect-runner \
             -Dexec.mainClass="demo.HourlyTeamScore" \
             -Dexec.args="--runner=DirectRunner --input=$BEAM_LOCAL_FILE --outputPrefix=data/count"

    mvn clean package exec:java -Pdataflow-runner \
            -Dexec.mainClass="demo.HourlyTeamScore" \
            -Dexec.args="--runner=DataflowRunner \
                         --input=$GCP_INPUT_FILE \
                         --outputPrefix=$GCP_OUTPUT_FILE/dataflow/hourly/res \
                         --project=$GCP_PROJECT"

    mvn clean package -Pspark-runner
    spark-submit \
            --class demo.HourlyTeamScore \
            ./target/portability-demo-bundled-spark.jar 
            --runner=SparkRunner 
            --input=data/demo-file.csv         
            --outputPrefix=data/hourlyTeamScore

    gcloud dataproc jobs submit spark \
            --project=$GCP_PROJECT \
            --cluster gaming-spark \
            --properties spark.default.parallelism=200 \
            --class demo.HourlyTeamScore \
            --jars ./target/portability-demo-bundled-spark.jar \
            -- \
            --runner=spark \
            --input=$GCP_INPUT_FILE \
            --outputPrefix=$GCP_OUTPUT_FILE/spark/hourly/res

Just like the `UserScore` pipeline, you can submit this pipeline to flink via the UI, only changing
the class to `demo.HourlyTeamScore`, and arguments:

    --parallelism=20 --input=gs://apache-beam-demo/data/gaming* 
    --outputPrefix=gs://beam-workshop-outputs/yourusername/flink/hourly/scores
    --runner=flink

## The LeaderBoard pipeline
This pipeline is interesting because it's our first streaming pipeline. We want to keep an
up-to-date leaderboard so that it can be displayed on our website. We want to report the 
most up-to date results, but we also want to update them as time goes by and we get more
data.

    mvn clean package exec:java -Pdirect-runner \
            -Dexec.mainClass="demo.LeaderBoard" \
            -Dexec.args="--runner=DirectRunner \
                         --topic=projects/$GCP_PROJECT/topics/$PUBSUB_TOPIC \
                         --outputPrefix=data/outputfiles"

    mvn clean package exec:java -Pdataflow-runner \
            -Dexec.mainClass="demo.LeaderBoard" \
            -Dexec.args="--runner=DataflowRunner \
                         --topic=projects/$GCP_PROJECT/topics/$PUBSUB_TOPIC \
                         --outputPrefix=$GCP_OUTPUT_FILE/dataflow/leader/res"

To submit this pipeline to the Flink UI, you can do the same as previous pipeline, with class
`demo.LeaderBoard`, and arguments: 

    --parallelism=20 
    --topic=projects/$GCP_PROJECT/topics/$PUBSUB_TOPIC 
    --outputPrefix=gs://beam-workshop-outputs/yourusername/flink/leader/board
    --runner=flink

## Injector

On a new "injector VM", install Maven (minimum 3.3.1), git, and OpenJDK 7. The
injector can inject data to a Kafka Topic, a Pubsub Topic, or a local file. To
test your own changes, you can generate a local file with data:

    mvn clean compile exec:java@injector -Dexec.args="--fileName=$BEAM_LOCAL_FILE"

Press `Ctrl-C` when you are pleased with the amount of data generated.

To stream data to PubSub, use the flags `--gcpProject=$GCP_PROJECT --pubsubTopic=$PUBSUB_TOPIC` instead of `--fileName`.
To stream data to Kafka, you may run the Injector without any arguments.
