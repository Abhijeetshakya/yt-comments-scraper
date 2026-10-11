from datetime import datetime, timezone


FIELDS = ('commentId', 'commentUrl', 'text', 'publishedAt', 'updatedAt', 'likeCount',
          'authorName', 'isCreatorReply', 'isReply', 'parentId', 'threadRootId',
          'parentRelationship', 'replyCount', 'fetchedReplyCount', 'isPinned',
          'heartedByCreator', 'videoId', 'videoUrl', 'videoTitle', 'videoChannelId',
          'source', 'publicDetailsSource', 'sort', 'scrapedAt', 'publishedAtPrecision')


def now():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def base(comment_id, vid, meta, sort):
    row = dict.fromkeys(FIELDS)
    row.update(commentId=comment_id, commentUrl=f'{vid.url}&lc={comment_id}',
               videoId=vid.id, videoUrl=vid.url, videoTitle=meta.get('title'),
               videoChannelId=meta.get('channelId'), sort=sort, scrapedAt=now())
    return row


def api_row(raw, vid, meta, sort, root=None, reply_count=None):
    s = raw['snippet']
    row = base(raw['id'], vid, meta, sort)
    author_id = s.get('authorChannelId', {}).get('value')
    creator = ((author_id == meta['channelId']) if author_id and meta.get('channelId') else
               s.get('_creatorMatch'))
    row.update(text=s.get('textDisplay'), publishedAt=s.get('publishedAt'), updatedAt=s.get('updatedAt'),
               likeCount=s.get('likeCount'), authorName=s.get('authorDisplayName'),
               isCreatorReply=creator if root else False, isReply=bool(root), parentId=root,
               threadRootId=root or raw['id'], parentRelationship='thread_root' if root else 'not_applicable',
               replyCount=reply_count if not root else None, fetchedReplyCount=0 if not root else None,
               source='youtube_api', publishedAtPrecision='exact' if s.get('publishedAt') else None)
    return row


def public_row(raw, vid, meta, sort, roots):
    row = base(raw['id'], vid, meta, sort)
    parent = raw.get('parent')
    if parent == 'root':
        parent = None
    root = roots.get(parent, parent) if parent else raw['id']
    roots[raw['id']] = root
    timestamp = raw.get('timestamp')
    row.update(text=raw.get('text'), publishedAt=(datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
               .replace('+00:00', 'Z')) if timestamp is not None else None,
               likeCount=raw.get('like_count'), authorName=raw.get('author'),
               isCreatorReply=raw.get('author_is_uploader') if parent else False, isReply=bool(parent),
               parentId=parent, threadRootId=root, parentRelationship='immediate' if parent else 'not_applicable',
               replyCount=raw.get('reply_count') if not parent else None,
               fetchedReplyCount=0 if not parent else None, isPinned=raw.get('is_pinned'),
               heartedByCreator=raw.get('is_favorited'), source='yt_dlp', publicDetailsSource='yt_dlp',
               publishedAtPrecision='estimated' if timestamp is not None else None)
    return row


def recount(rows):
    counts = {}
    for row in rows:
        if row['isReply']:
            counts[row['threadRootId']] = counts.get(row['threadRootId'], 0) + 1
    for row in rows:
        if not row['isReply']:
            row['fetchedReplyCount'] = counts.get(row['commentId'], 0)
