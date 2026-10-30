# Edith
- ad decisioning service
- full name: Event-Driven Intelligent Targeting Hub

<img src="/edith.gif" width="200" alt="Edith GIF">

## What's going on here

Edith is one of three small projects. Together, they pretend to be a video streaming service. Everything is simulated, so the ads are just little test videos. But the pieces really do talk to each other.

- **[Margo](https://github.com/marlware/margo)** checks new videos before anyone can watch them
- **Edith** (this one) picks which ad to show and counts how many times each ad was seen
- **[Agnes](https://github.com/marlware/agnes)** streams the videos to viewers and plays the ads

## So what does Edith do

Edith has two jobs. The first is picking an ad.

- Agnes asks Edith for an ad when a viewer is about to watch something
- Edith looks up what that viewer is into
- Then it finds an ad that fits
- In the demo data, `alice` likes sports and `bob` likes tech
- Anybody Edith doesn't know gets a general ad
- The interests and ads live in Redis, which is a super fast lookup table

The second job is counting views.

- Agnes drops a note on a message board (Kafka) when an ad finishes playing
- The note says "this person just saw this ad"
- Edith reads those notes and saves them
- A separate counting job keeps a running total for each ad
- The totals are available at `/stats`

## Why these technologies

Every tool here earns its spot. Here's what each one does for Edith.

- **Java and Spring Boot** make it quick to build the web service that picks ads
- **Redis** keeps viewer interests and ads in memory, so Edith can find a match in the moment a video starts
- **Kafka** lets Agnes report a view and move on without waiting for Edith
- Kafka also lets Edith work through a burst of views at its own pace
- **Flink** is built for counting events as they stream in, so it keeps a running total for each ad
- **Scala** is the language the counting job is written in, and it runs on the same Java platform as Flink
- **sbt** builds that job into a single file you can hand to Flink
- **Docker** runs the Flink cluster in containers, so the job works no matter which Java is on your computer

## Trying it out

Here's what you'll need.

- Java
- Docker
- The stack from the Margo repo running (`docker compose up -d` over in `margo`)

Then try these.

- Start Edith with `./mvnw spring-boot:run` (it listens on port 8082)
- Ask for an ad with `curl "localhost:8082/ads?viewerId=alice"`
- Try `bob` next
- Then try `somebody-new` to see the general ad
- Peek at the notes from Agnes with `curl localhost:8082/impressions`
- To see it work for real, run Agnes and play a video
- Or send a pile of fake views with the stress test in the Agnes repo

## The counting job

The counting job is built with Scala and Flink. It lives in the `flink` folder. It reads the same stream of "ad was seen" notes. Then it keeps a running count for each ad and saves the totals in Redis.

After the 10,000-view stress test, the counts add up to 10,000 at `localhost:8082/stats`.

To start it:

- Make sure the Margo stack is up, since that's where the Flink cluster runs
- Look at the cluster at http://localhost:8083 if you're curious
- Run `flink/submit.sh`, which builds the job and hands it to the cluster
- Have sbt installed
- Have a Java between 17 and 21 for the build, because newer ones like 25 don't work here

## Good to know

- Starting Edith loads the demo ads and viewers into Redis automatically
- There's nothing to set up
- The list at `/impressions` is kept in memory, so a restart clears it
- The Flink totals in Redis stay put
- The counting job starts from the beginning of the notes every time
- So restarting it gives the same totals instead of double counting
- The endpoints are `GET /ads`, `GET /impressions`, and `GET /stats`
