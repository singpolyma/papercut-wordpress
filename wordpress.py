# Copyright (c) 2010 Stephen Paul Weber. Based on work by Joao Prado Maia.
# Licensed under the ISC License

import MySQLdb
import time
from mimify import mime_encode_header, mime_decode_header
import re
import settings
import mime
import strutil
import os.path

try:
    import html2text
except ImportError:
    html2text = None # Optional, GPL

# patch by Andreas Wegmann <Andreas.Wegmann@VSA.de> to fix the handling of unusual encodings of messages
q_quote_multiline = re.compile("=\?(.*?)\?[qQ]\?(.*?)\?=.*?=\?\\1\?[qQ]\?(.*?)\?=", re.M | re.S)
# we don't need to compile the regexps everytime..
doubleline_regexp = re.compile("^\.\.", re.M)
singleline_regexp = re.compile("^\.", re.M)
from_regexp = re.compile("^From:(.*)<(.*)>", re.M)
subject_regexp = re.compile("^Subject:(.*)", re.M)
references_regexp = re.compile("^References:(.*)<(.*)>", re.M)
lines_regexp = re.compile("^Lines:(.*)", re.M)

class Papercut_Storage:
    """
    Storage Backend interface for the Wordpress blog software
    
    This is the interface for Wordpress running on a MySQL database. For more information
    on the structure of the 'storage' package, please refer to the __init__.py
    available on the 'storage' sub-directory.
    """

    def __init__(self):
        self.conn = MySQLdb.connect(host=settings.dbhost, db=settings.dbname, user=settings.dbuser, passwd=settings.dbpass, charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS wp_newsgroup_meta(
                               article_number BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                               message_id CHAR(255) UNIQUE NOT NULL,
                               id BIGINT NOT NULL, tbl CHAR(50), newsgroup CHAR(255),
                               CONSTRAINT UNIQUE INDEX id_table (id, tbl),
                               INDEX newsgroup (newsgroup)
                               )""")
        self.update_newsgroup_meta()

    def get_message_body(self, headers):
        """Parses and returns the most appropriate message body possible.
        
        The function tries to extract the plaintext version of a MIME based
        message, and if it is not available then it returns the html version.        
        """
        return mime.get_text_message(headers)

    def quote_string(self, text):
        """Quotes strings the MySQL way."""
        return text.replace("'", "\\'")

    def group_exists(self, group_name):
        return (group_name == 'blog.singpolyma') # TODO

    def update_newsgroup_meta(self):
        group = 'blog.singpolyma' # TODO
        meta_table = self.get_table_name(table_name='newsgroup_meta')
        posts_table = self.get_table_name(table_name='posts')
        comments_table = self.get_table_name(table_name='comments')
        stmt = """ INSERT INTO wp_newsgroup_meta (id, tbl, message_id, newsgroup)
                   SELECT ID, tbl, message_id, '%s' FROM (
                   (SELECT
                       a.ID, 'wp_posts' AS tbl,
                       CONCAT('<post-', a.ID, '@%s>') AS message_id,
                       post_date_gmt AS datestamp
                   FROM
                       wp_posts a LEFT JOIN wp_newsgroup_meta b ON a.ID=b.id AND b.tbl='wp_posts'
                   WHERE
                       isNULL(b.id) AND post_type='post' AND post_status='publish'
                   ) UNION (
                   SELECT
                       comment_ID as ID, 'wp_comments' AS tbl,
                       CONCAT('<comment-', comment_ID, '@%s>') AS message_id,
                       comment_date_gmt AS datestamp
                   FROM
                       wp_posts c, wp_comments a LEFT JOIN wp_newsgroup_meta b ON comment_ID=b.id AND b.tbl='wp_comments'
                   WHERE
                       a.comment_post_ID=c.ID AND
                       isNULL(b.id) AND comment_approved='1' AND
                       post_type='post' AND post_status='publish'
                   )
                   ORDER BY datestamp) t
               """.replace('wp_posts', posts_table).replace('wp_comments', comments_table).replace('wp_newsgroup_meta', meta_table) % (group, settings.nntp_hostname, settings.nntp_hostname)
        self.cursor.execute(stmt)

    def article_exists(self, group_name, style, range):
        self.update_newsgroup_meta()
        table_name = self.get_table_name(table_name='newsgroup_meta')
        stmt = """
                SELECT
                    COUNT(*) AS total
                FROM
                    %s
                WHERE
                    newsgroup='%s' AND """ % (table_name, group_name)
        if style == 'range':
            stmt = "%s AND article_number > %s" % (stmt, range[0])
            if len(range) == 2:
                stmt = "%s AND article_number < %s" % (stmt, range[1])
        else:
            stmt = "%s AND article_number = %s" % (stmt, range[0])
        self.cursor.execute(stmt)
        return self.cursor.fetchone()[0]

    def get_first_article(self, group_name):
        self.update_newsgroup_meta()
        table_name = self.get_table_name(table_name='newsgroup_meta')
        stmt = """
                SELECT
                    IF(MIN(message_num) IS NULL, 0, MIN(message_num)) AS first_article
                FROM
                    %s
                WHERE
                   newsgroup='%s'""" % (table_name, group_name)
        num_rows = self.cursor.execute(stmt)
        return self.cursor.fetchone()[0]

    def get_group_stats(self, group_name):
        self.update_newsgroup_meta()
        table_name = self.get_table_name(table_name='newsgroup_meta')
        stmt = """
                SELECT
                   COUNT(article_number) AS total,
                   IF(MAX(article_number) IS NULL, 0, MAX(article_number)) AS maximum,
                   IF(MIN(article_number) IS NULL, 0, MIN(article_number)) AS minimum
                FROM
                    %s
                WHERE
                    newsgroup='%s'""" % (table_name, group_name)
        self.cursor.execute(stmt)
        total, maxi, mini = self.cursor.fetchone()
        return (total, mini, maxi, group_name)

    def get_table_name(self, group_name=None, table_name=None):
        if not table_name:
            table_name = 'posts'
        return 'wp_' + table_name # TODO

    def get_message_id(self, msg_num, group, table=None):
        table_name = self.get_table_name(table_name='newsgroup_meta')
        compar = table and 'id' or 'article_number'
        stmt = """
               SELECT
                   message_id
               FROM
                   %s
               WHERE
                   newsgroup='%s' AND %s=%s
               """ % (table_name, group, compar, int(msg_num))
        if table:
            stmt += " AND tbl='%s'" % self.get_table_name(table_name=table)
        self.cursor.execute(stmt)
        return self.cursor.fetchone()[0]

    def get_article_sql(self):
        meta_table = self.get_table_name(table_name='newsgroup_meta')
        posts_table = self.get_table_name(table_name='posts')
        comments_table = self.get_table_name(table_name='comments')
        stmt = """
                SELECT M.article_number,S.*,M.message_id FROM (
                (SELECT
                    A.ID as ID,
                    display_name,
                    user_email,
                    post_title,
                    UNIX_TIMESTAMP(post_date_gmt) AS datestamp,
                    post_content,
                    post_parent,
                    0 AS comment_parent
                FROM
                    wp_posts A,
                    wp_users
                WHERE
                    A.post_type='post' AND A.post_status='publish' AND
                    A.post_author=wp_users.ID
               ) UNION (
               SELECT
                   comment_ID AS ID,
                   IF(user_id = 0, comment_author, display_name) as display_name,
                   IF(user_id = 0, comment_author_email, user_email) as user_email,
                   CONCAT('Re: ', post_title) as post_title,
                   UNIX_TIMESTAMP(comment_date_gmt) AS datestamp,
                   comment_content AS post_content,
                   comment_post_ID AS post_parent,
                   comment_parent
               FROM
                   wp_comments A LEFT OUTER JOIN
                   wp_users ON user_id=wp_users.ID,
                   wp_posts
               WHERE
                   comment_post_ID=wp_posts.ID AND
                   comment_approved='1' AND
                   wp_posts.post_type='post' AND wp_posts.post_status='publish'
               ) ) S, wp_newsgroup_meta M
               WHERE
                   M.id=S.ID
               """.replace('wp_posts', posts_table).replace('wp_comments', comments_table).replace('wp_newsgroup_meta',     meta_table)
        return stmt

    def get_NEWGROUPS(self, ts, group='%'):
        return None # TODO

    def get_NEWNEWS(self, ts, group='*'):
        self.update_newsgroup_meta()
        group = 'blog.singpolyma' # TODO
        meta_table = self.get_table_name(table_name='newsgroup_meta')
        posts_table = self.get_table_name(table_name='posts')
        comments_table = self.get_table_name(table_name='comments')
        ts = int(time.mktime(ts))
        stmt = """
                (SELECT
                    article_number
                FROM
                    wp_posts, wp_newsgroup_meta
                WHERE
                    wp_posts.ID=wp_newsgroup_meta.id AND wp_newsgroup_meta.tbl='wp_posts' AND
                    post_type='post' AND post_status='publish' AND
                    UNIX_TIMESTAMP(post_date_gmt) >= %s
                ) UNION (
                SELECT
                   article_number
                FROM
                    wp_comments,
                    wp_posts,
                    wp_newsgroup_meta
                WHERE
                    comment_ID=wp_newsgroup_meta.id AND wp_newsgroup_meta.tbl='wp_comments' AND
                    comment_post_ID=wp_posts.ID AND
                    post_type='post' AND post_status='publish' AND
                    comment_approved = '1' AND
                    UNIX_TIMESTAMP(comment_date_gmt) >= %s
                )
                ORDER BY
                    article_number ASC""" % (ts, ts)
        stmt = stmt.replace('wp_posts', posts_table).replace('wp_comments', comments_table).replace('wp_newsgroup_meta',  meta_table)
        self.cursor.execute(stmt)
        result = list(self.cursor.fetchall())
        return "\r\n".join(["%s" % k for k in result])

    def get_GROUP(self, group_name):
        stats = self.get_group_stats(group_name)
        return (stats[0], stats[1], stats[2])

    def get_LIST(self, username=""):
        lists = []
        stats = self.get_group_stats('blog.singpolyma')
        lists.append("%s %s %s y" % ('blog.singpolyma', stats[2], stats[1])) # TODO
        return "\r\n".join(lists)

    def get_STAT(self, group_name, id):
        meta_table = self.get_table_name(table_name='newsgroup_meta')
        stmt = """
                SELECT
                    article_number
                FROM
                    %s
                WHERE
                    newsgroup='%s' AND
                    article_number=%s""" % (meta_table, group_name, id)
        return self.cursor.execute(stmt)

    def get_ARTICLE(self, group_name, id, headers_only=False, body_only=False):
        stmt = self.get_article_sql()
        if str(id).count('<') > 0 or str(id).count('@') > 0:
            id = self.quote_string(id)
            stmt += " AND message_id='%s'" % (id,)
        else:
            id = int(id)
            stmt += " AND article_number=%s" % (id,)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        result = list(self.cursor.fetchone())
        if not body_only:
            if len(result[3]) == 0:
                author = result[2]
            else:
                author = "%s <%s>" % (result[2], result[3])
            formatted_time = strutil.get_formatted_time(time.localtime(result[5]))
            headers = []
            headers.append("Path: %s" % (settings.nntp_hostname))
            headers.append("From: %s" % (author))
            headers.append("Newsgroups: %s" % (group_name))
            headers.append("Date: %s" % (formatted_time))
            headers.append("Subject: %s" % (result[4]))
            headers.append("Message-ID: %s" % (result[9]))
            headers.append("Xref: %s %s:%s" % (settings.nntp_hostname, group_name, result[0]))
            parent = []
            if result[7] != 0:
                parent.append(self.get_message_id(result[7], group_name, 'posts'))
            if result[8] != 0:
                parent.append(self.get_message_id(result[8], group_name, 'comments'))
            if len(parent) > 0:
                headers.append("References: " + ', '.join(parent))
                headers.append("In-Reply-To: " + parent.pop())
            headers.append('Content-Type: text/plain; charset=utf-8')
        if headers_only:
            return "\r\n".join(headers)
        if html2text:
            body = html2text.html2text(result[6].encode('utf-8').replace("\r\n", "\n").replace("\r", "\n").replace("\n\n", "</p><p>")).encode('utf-8')
        else:
            body = strutil.format_body(result[6].encode('utf-8'))
        if body_only:
            return body
        return ("\r\n".join(headers).encode('utf-8'), body)

    def get_LAST(self, group_name, current_id):
        meta_table = self.get_table_name(table_name='newsgroup_meta')
        stmt = """
               SELECT
                   article_number
               FROM
                   %s
               WHERE
                   newsgroup='%s' AND article_number < %s
               ORDER BY
                   ID DESC
               LIMIT 0, 1
               """ % (meta_table, group_name, current_id)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        return self.cursor.fetchone()[0]

    def get_NEXT(self, group_name, current_id):
        meta_table = self.get_table_name(table_name='newsgroup_meta')
        stmt = """
               SELECT
                   article_number
               FROM
                   %s
               WHERE
                   newsgroup='%s' AND article_number > %s
               ORDER BY
                   ID ASC
               LIMIT 0, 1
               """ % (meta_table, group_name, current_id)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        return self.cursor.fetchone()[0]

    def get_HEAD(self, group_name, id):
        return self.get_ARTICLE(group_name, id, headers_only=True)

    def get_BODY(self, group_name, id):
        return self.get_ARTICLE(group_name, id, body_only=True)

    def get_XOVER(self, group_name, start_id, end_id='ggg'):
        self.update_newsgroup_meta()
        stmt = self.get_article_sql()
        stmt += " AND article_number >= %s" % (start_id,)
        if end_id != 'ggg':
            stmt += " AND article_number <= %s" % (end_id,)
        self.cursor.execute(stmt)
        result = list(self.cursor.fetchall())
        overviews = []
        for row in result:
            if html2text:
                body = html2text.html2text(row[6].encode('utf-8')).encode('utf-8')
            else:
                body = strutil.format_body(row[6].encode('utf-8'))
            if row[3] == '':
                author = row[2]
            else:
                author = "%s <%s>" % (row[2], row[3])
            formatted_time = strutil.get_formatted_time(time.localtime(row[5]))
            message_id = row[9]
            line_count = body.count("\n")
            xref = 'Xref: %s %s:%s' % (settings.nntp_hostname, group_name, row[0])
            parent = []
            if row[7] != 0:
                parent.append(self.get_message_id(row[7], group_name, 'posts'))
            if row[8] != 0:
                parent.append(self.get_message_id(row[8], group_name, 'comments'))
            reference = ', '.join(parent)
            # message_number <tab> subject <tab> author <tab> date <tab> message_id <tab> reference <tab> bytes <tab> lines <tab> xref
            overviews.append("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (row[0], row[4], author, formatted_time, message_id, reference, len(body), line_count, xref))
        return "\r\n".join(overviews)

    def get_XPAT(self, group_name, header, pattern, start_id, end_id='ggg'):
        return None # TODO: really broken
        # XXX: need to actually check for the header values being passed as
        # XXX: not all header names map to column names on the tables
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    A.ID,
                    post_parent,
                    display_name,
                    user_email,
                    post_title,
                    UNIX_TIMESTAMP(post_date_gmt) AS datestamp,
                    post_content
                FROM
                    %s A, 
                    wp_users
                WHERE
                    A.post_type='post' AND A.post_status='publish' AND
                    %s REGEXP '%s' AND
                    post_author = wp_users.ID AND
                    A.ID >= %s""" % (table_name, header, strutil.format_wildcards(pattern), start_id)
        if end_id != 'ggg':
            stmt = "%s AND A.id <= %s" % (stmt, end_id)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        result = list(self.cursor.fetchall())
        hdrs = []
        for row in result:
            if header.upper() == 'SUBJECT':
                hdrs.append('%s %s' % (row[0], row[4]))
            elif header.upper() == 'FROM':
                # XXX: totally broken with empty values for the email address
                hdrs.append('%s %s <%s>' % (row[0], row[2], row[3]))
            elif header.upper() == 'DATE':
                hdrs.append('%s %s' % (row[0], strutil.get_formatted_time(time.localtime(result[5]))))
            elif header.upper() == 'MESSAGE-ID':
                hdrs.append(row[0] + ' ' + self.get_message_id(row[0], group_name))
            elif (header.upper() == 'REFERENCES') and (row[1] != 0):
                hdrs.append(row[0] + ' ' + self.message_id(row[1], group_name))
            elif header.upper() == 'BYTES':
                hdrs.append('%s %s' % (row[0], len(row[6])))
            elif header.upper() == 'LINES':
                hdrs.append('%s %s' % (row[0], len(row[6].split('\n'))))
            elif header.upper() == 'XREF':
                hdrs.append('%s %s %s:%s' % (row[0], settings.nntp_hostname, group_name, row[0]))
        if len(hdrs) == 0:
            return ""
        else:
            return "\r\n".join(hdrs)

    def get_LISTGROUP(self, group_name):
        self.update_newsgroup_meta()
        meta_table = self.get_table_name(table_name='newsgroup_meta')
        stmt = """
               SELECT
                   article_number
               FROM
                   %s
               WHERE
                   newsgroup='%s'
               """ % (meta_table, group_name)
        self.cursor.execute(stmt)
        result = list(self.cursor.fetchall())
        return "\r\n".join(["%s" % k for k in result])

    def get_XGTITLE(self, pattern=None):
        return "blog.singpolyma Singpolyma" # TODO

    def get_XHDR(self, group_name, header, style, range):
        self.update_newsgroup_meta()
        stmt = self.get_article_sql()

        if style == 'range':
            stmt += ' AND article_number >= %s' % (range[0],)
            if len(range) == 2:
                stmt += ' AND article_number <= %s' % (range[1])
        else:
            stmt += ' AND article_number = %s' % (range[0],)
        if self.cursor.execute(stmt) == 0:
            return None
        result = self.cursor.fetchall()
        hdrs = []
        for row in result:
            parent = []
            if row[7] != 0:
                parent.append(self.get_message_id(row[7], group_name, 'posts'))
            if row[8] != 0:
                parent.append(self.get_message_id(row[8], group_name, 'comments'))
            if header.upper() == 'SUBJECT':
                hdrs.append('%s %s' % (row[0], row[4]))
            elif header.upper() == 'FROM':
                hdrs.append('%s %s <%s>' % (row[0], row[2], row[3]))
            elif header.upper() == 'DATE':
                hdrs.append('%s %s' % (row[0], strutil.get_formatted_time(time.localtime(result[5]))))
            elif header.upper() == 'MESSAGE-ID':
                hdrs.append(row[0] + ' ' + row[9])
            elif (header.upper() == 'REFERENCES') and len(parent) > 0:
                hdrs.append('%s %s' % (row[0], ', '.join(parent)))
            elif header.upper() == 'BYTES':
                hdrs.append('%s %s' % (row[0], len(row[6])))
            elif header.upper() == 'LINES':
                hdrs.append('%s %s' % (row[0], len(row[6].split('\n'))))
            elif header.upper() == 'XREF':
                hdrs.append('%s %s %s:%s' % (row[0], settings.nntp_hostname, group_name, row[0]))
        if len(hdrs) == 0:
            return ""
        else:
            return "\r\n".join(hdrs)

    def do_POST(self, group_name, lines, ip_address, username=''):
        return None # TODO, below code from other engine, just for reference
        table_name = self.get_table_name(group_name)
        body = self.get_message_body(lines)
        author, email = from_regexp.search(lines, 0).groups()
        subject = subject_regexp.search(lines, 0).groups()[0].strip()
        # patch by Andreas Wegmann <Andreas.Wegmann@VSA.de> to fix the handling of unusual encodings of messages
        lines = mime_decode_header(re.sub(q_quote_multiline, "=?\\1?Q?\\2\\3?=", lines))
        if lines.find('References') != -1:
            # get the 'modifystamp' value from the parent (if any)
            references = references_regexp.search(lines, 0).groups()
            parent_id, void = references[-1].strip().split('@')
            stmt = """
                    SELECT
                        IF(MAX(id) IS NULL, 1, MAX(id)+1) AS next_id
                    FROM
                        %s""" % (table_name)
            num_rows = self.cursor.execute(stmt)
            if num_rows == 0:
                new_id = 1
            else:
                new_id = self.cursor.fetchone()[0]
            stmt = """
                    SELECT
                        id,
                        thread,
                        modifystamp
                    FROM
                        %s
                    WHERE
                        approved='Y' AND
                        id=%s
                    GROUP BY
                        id""" % (table_name, parent_id)
            num_rows = self.cursor.execute(stmt)
            if num_rows == 0:
                return None
            parent_id, thread_id, modifystamp = self.cursor.fetchone()
        else:
            stmt = """
                    SELECT
                        IF(MAX(id) IS NULL, 1, MAX(id)+1) AS next_id,
                        UNIX_TIMESTAMP()
                    FROM
                        %s""" % (table_name)
            self.cursor.execute(stmt)
            new_id, modifystamp = self.cursor.fetchone()
            parent_id = 0
            thread_id = new_id
        stmt = """
                INSERT INTO
                    %s
                (
                    id,
                    datestamp,
                    thread,
                    parent,
                    author,
                    subject,
                    email,
                    host,
                    email_reply,
                    approved,
                    msgid,
                    modifystamp,
                    userid
                ) VALUES (
                    %s,
                    NOW(),
                    %s,
                    %s,
                    '%s',
                    '%s',
                    '%s',
                    '%s',
                    'N',
                    'Y',
                    '',
                    %s,
                    0
                )
                """ % (table_name, new_id, thread_id, parent_id, self.quote_string(author.strip()), self.quote_string(subject), self.quote_string(email), ip_address, modifystamp)
        if not self.cursor.execute(stmt):
            return None
        else:
            # insert into the '*_bodies' table
            stmt = """
                    INSERT INTO
                        %s_bodies
                    (
                        id,
                        body,
                        thread
                    ) VALUES (
                        %s,
                        '%s',
                        %s
                    )""" % (table_name, new_id, self.quote_string(body), thread_id)
            if not self.cursor.execute(stmt):
                # delete from 'table_name' before returning..
                stmt = """
                        DELETE FROM
                            %s
                        WHERE
                            id=%s""" % (table_name, new_id)
                self.cursor.execute(stmt)
                return None
            else:
                # alert forum moderators
                self.send_notifications(group_name, new_id, thread_id, parent_id, author.strip(), email, subject, body)
                return 1
