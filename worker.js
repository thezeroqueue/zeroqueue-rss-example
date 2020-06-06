const Queue = require("bull");
const Parser = require("rss-parser");
const Airtable = require("airtable");

const queue = new Queue("Fetch Articles", process.env.REDIS_URL);
const parser = new Parser();
const base = new Airtable({ apiKey: "API_KEY" }).base("BASE_ID");

queue.process("*", async (job, done) => {
  const { source, url } = job.data;

  try {
    const feed = await parser.parseURL(url);
    await Promise.all(
      feed.items.map((item) =>
        base("Articles").create(
          [
            {
              fields: {
                Name: item.title,
                Source: source,
                Link: item.link,
                Timestamp: new Date(item.pubDate).toISOString(),
              },
            },
          ],
          { typecast: true }
        )
      )
    );
  } catch (error) {
    job.log(error);
  }

  job.progress(100);
  done(null, null);
});
