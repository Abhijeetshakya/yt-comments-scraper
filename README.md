# Fast YouTube Comments Scraper

Collect YouTube comments and replies from multiple videos with custom limits, sorting, and publication date filters. Export structured results for audience research, feedback analysis, and engagement tracking.

[Open the Actor on Apify](https://console.apify.com/actors/jCpWm0IFhnTdOo4YU)

## Features

- **Multiple videos:** Add YouTube video links and choose a separate comment amount for each.
- **Custom limits:** Defaults to **100 records per video**, with an optional limit for the entire run. Comments and replies count together.
- **Replies and threads:** Include replies, identify parent comments, and optionally limit replies per thread.
- **Comment sorting:** Choose **Newest first** or **Top comments**.
- **Publication date filter:** Collect comments published on or after a date, or use an age such as `1 day` or `2 days`. Selecting a date filter automatically switches sorting to Newest first.
- **Engagement details:** Get comment likes and reply counts, with optional pinned indicators, creator hearts, and nested reply relationships when available.
- **Creator replies:** Identify replies written by the video's creator when this can be established.
- **JSON and CSV exports:** Download comment records for spreadsheets, dashboards, or further analysis.
- **Separate video summaries:** Review collected counts, completion status, elapsed time, limits reached, and warnings.

## Use cases

- Understand audience feedback on videos, products, or campaigns.
- Research common questions, opinions, and recurring topics.
- Compare discussion and engagement across several videos.
- Review recent comments for ongoing audience monitoring.
- Collect structured comment data for research and reporting.

## How to use

1. Open the Actor on Apify and add your video links.
2. Set the maximum number of records for each video, or keep the default of 100.
3. Choose sorting, replies, and any date or engagement filters.
4. Start the run and review the results.
5. Export the comment dataset as JSON or CSV. Check the separate video summaries for completion details.

## Input options

| Option | Default | Purpose |
| --- | --- | --- |
| Videos and comment amounts | 100 records per video | Add each URL alongside its requested amount. |
| Comment sorting | Newest first | Select Newest first or Top comments. |
| Scrape comments published after | Unset | Include comments and replies published on or after the cutoff. |
| Include replies | On | Collect replies within selected comment threads. |
| Maximum replies per thread | Unset | Set an optional reply cap for each thread. |
| Maximum records across the run | Unset | Set a combined limit across all videos. |
| Additional engagement details | Off | Select pinned comments, creator hearts, or nested reply relationships. These can increase run time. |
| Verify empty results | On | Check unexpected empty results before reporting that no comments were found. |
| Simultaneous videos | 3 | Choose how many videos to process at once. |
| Processing timeout per video | 180 seconds | Limit the active processing time for each video. |

### Date filter examples

| Enter | Comments included |
| --- | --- |
| `2026-10-01` | October 1, 2026 onward, including that date. |
| `1 day` | Today's comments. |
| `2 days` | Yesterday's and today's comments. |
| Leave empty | No publication date filter. |

Date filters use **UTC calendar days** and publication dates, rather than last updated dates. Replies must meet the same cutoff and belong to qualifying parent threads; new replies on older parent threads are outside this mode. Comments with unavailable publication dates are excluded when a date filter is selected.

## Output

Each dataset record represents one comment or reply.

| Group | Included information |
| --- | --- |
| Comment | ID, direct comment link, full text, publication date, last updated date, and like count. |
| Author | Display name and creator reply indicator. |
| Thread | Top-level or reply indicator, parent ID, root thread ID, relationship type, total reply count, and collected reply count. |
| Optional engagement details | Pinned indicator, creator heart status, and nested reply relationships when established. |
| Video | Video ID, URL, title, and owner channel ID. |
| Collection | Sorting used, collection timestamp, and publication date precision. |

Unavailable values are returned as `null`. Some publication dates may be estimated and are labeled accordingly. Reply counts can exceed the number collected when a record limit is reached. Top-level comments carry total and collected reply counts; those fields are `null` on reply records.

Video summaries are stored separately from the comment dataset. They include the requested amount, collected comment and reply counts, status, elapsed time, applied date cutoff, limit flags, additional-detail status, and warnings. When comments exist but none meet the date filter, the status is `no_matching_comments`.

## Measured performance

A live Apify test on **October 11, 2026** collected **1,000 records across 10 videos**, limited to 100 records per video.

| Metric | Measured result |
| --- | ---: |
| Videos completed | 10 of 10 |
| Total records | 1,000 |
| Top-level comments | 630 |
| Replies | 370 |
| Comment collection time | 3.32 seconds |
| Total run time, including startup | 6.64 seconds |
| First saved results after collection began | 0.88 seconds |

This test used build **0.1.3**, 1 GB of allocated memory, Newest first sorting, replies enabled, and additional engagement details disabled. All sample videos were music videos. These figures describe one measured run; performance varies with video activity, reply depth, selected limits, and optional details.

[View the measured run on Apify](https://console.apify.com/actors/runs/OTyn7eJ4pq0YVF5kP)
