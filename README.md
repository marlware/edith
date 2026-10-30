# Edith

The ad picker

## What's going on here

Edith is one of three small projects that together pretend to be a video streaming service, kind of like a baby version of what sits behind Netflix or YouTube. Everything is simulated, so the ads are just little test videos, but the way the pieces talk to each other is the real deal.

- **[Margo](https://github.com/marlware/margo)** checks new videos before anyone can watch them
- **[Agnes](https://github.com/marlware/agnes)** is the part viewers actually talk to, since it streams the videos and shows ads
- **Edith** (this one) decides which ad to show and keeps count of how many times each one was seen

## So what does Edith do

Edith has two jobs.

The first job is picking an ad. When a viewer is about to watch something, Agnes asks Edith for an ad, and Edith looks up what that viewer is into and finds one that fits. In the demo data, `alice` likes sports and `bob` likes tech, and anybody Edith doesn't know gets a general-purpose ad. The interests and ads live in Redis, which is a super fast lookup table, so finding a match is quick.

The second job is counting views. Once an ad finishes playing, Agnes drops a note on a message board (Kafka) saying "this person just saw this ad". Edith reads those notes and saves them, and a separate counting job built with Scala and Flink keeps a running total for each ad. The totals are available at `/stats`.

## Trying it out

You will need Java, Docker, and the stack from the Margo repo running (`docker compose up -d` over in `margo`). Then:

- Start Edith with `./mvnw spring-boot:run`, which listens on port 8082
- Ask for an ad with `curl "localhost:8082/ads?viewerId=alice"`, then try `bob`, then try `somebody-new`
- Peek at the notes from Agnes with `curl localhost:8082/impressions`
- To see it work for real, run Agnes too and play a video, or send it a pile of fake views with the stress test in the Agnes repo

## The counting job

The Scala and Flink part lives in the `flink` folder. It listens to the same stream of "ad was seen" notes and keeps a running count per ad, then saves the totals in Redis. After the 10,000-view stress test, you will see the counts add up to 10,000 at `localhost:8082/stats`.

To start it:

- Make sure the Margo stack is up, since that is where the Flink cluster runs (you can look at it at http://localhost:8083)
- Run `flink/submit.sh`, which builds the job and hands it over to the cluster
- Have sbt and a Java between 17 and 21 installed for the build, because newer Javas like 25 don't work for this part

## Good to know

- Starting Edith loads the demo ads and viewers into Redis automatically, so there is nothing to set up
- The list of views at `/impressions` is kept in memory, so restarting Edith clears it (the Flink totals in Redis stay)
- The counting job starts from the beginning of the notes every time it runs, so restarting it gives you the same totals instead of double counting
- The endpoints, if you want them, are `GET /ads`, `GET /impressions`, and `GET /stats`
