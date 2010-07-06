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
        self.conn = MySQLdb.connect(host=settings.dbhost, db=settings.dbname, user=settings.dbuser, passwd=settings.dbpass)
        self.cursor = self.conn.cursor()

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

    def article_exists(self, group_name, style, range):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    COUNT(*) AS total
                FROM
                    %s
                WHERE
                    post_type='post' AND post_status='publish'""" % (table_name)
        if style == 'range':
            stmt = "%s AND ID > %s" % (stmt, range[0])
            if len(range) == 2:
                stmt = "%s AND ID < %s" % (stmt, range[1])
        else:
            stmt = "%s AND ID = %s" % (stmt, range[0])
        self.cursor.execute(stmt)
        return self.cursor.fetchone()[0]

    def get_first_article(self, group_name):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    IF(MIN(ID) IS NULL, 0, MIN(ID)) AS first_article
                FROM
                    %s
                WHERE
                   post_type='post' AND post_status='publish'""" % (table_name)
        num_rows = self.cursor.execute(stmt)
        return self.cursor.fetchone()[0]

    def get_group_stats(self, group_name):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                   COUNT(ID) AS total,
                   IF(MAX(ID) IS NULL, 0, MAX(ID)) AS maximum,
                   IF(MIN(ID) IS NULL, 0, MIN(ID)) AS minimum
                FROM
                    %s
                WHERE
                    post_type='post' AND post_status='publish'""" % (table_name)
        num_rows = self.cursor.execute(stmt)
        total, max, min = self.cursor.fetchone()
        return (total, min, max, group_name)

    def get_table_name(self, group_name):
        return 'wp_posts' # TODO

    def get_message_id(self, msg_num, group):
        return '<%s@%s>' % (msg_num, group) # TODO

    def get_NEWGROUPS(self, ts, group='%'):
        return None # TODO

    def get_NEWNEWS(self, ts, group='*'):
        group = 'blog.singpolyma' # TODO
        table = 'wp_posts' # TODO 
        articles = []
        stmt = """
                SELECT
                    ID
                FROM
                    %s
                WHERE
                    post_type='post' AND post_status='publish' AND
                    UNIX_TIMESTAMP(datestamp) >= %s""" % (table, ts)
        num_rows = self.cursor.execute(stmt)
        ids = list(self.cursor.fetchall())
        for id in ids:
            articles.append(self.get_message_id(id, group))
        if len(articles) == 0:
            return ''
        else:
            return "\r\n".join(articles)

    def get_GROUP(self, group_name):
        stats = self.get_group_stats(group_name)
        return (stats[0], stats[1], stats[2])

    def get_LIST(self, username=""):
        lists = []
        stats = self.get_group_stats('blog.singpolyma')
        lists.append("%s %s %s y" % ('blog.singpolyma', stats[2], stats[1])) # TODO
        return "\r\n".join(lists)

    def get_STAT(self, group_name, id):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    ID
                FROM
                    %s
                WHERE
                    post_type='post' AND post_status='publish' AND
                    ID=%s""" % (table_name, id)
        return self.cursor.execute(stmt)

    def get_ARTICLE(self, group_name, id, headers_only=False, body_only=False):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    A.ID,
                    display_name,
                    user_email,
                    post_title,
                    UNIX_TIMESTAMP(post_date_gmt) AS datestamp,
                    post_content,
                    post_parent
                FROM
                    %s A,
                    wp_users
                WHERE
                    A.post_type='post' AND A.post_status='publish' AND
                    A.post_author=wp_users.ID AND
                    A.ID=%s""" % (table_name, id)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        result = list(self.cursor.fetchone())
        if not body_only:
            if len(result[2]) == 0:
                author = result[1]
            else:
                author = "%s <%s>" % (result[1], result[2])
            formatted_time = strutil.get_formatted_time(time.localtime(result[4]))
            headers = []
            headers.append("Path: %s" % (settings.nntp_hostname))
            headers.append("From: %s" % (author))
            headers.append("Newsgroups: %s" % (group_name))
            headers.append("Date: %s" % (formatted_time))
            headers.append("Subject: %s" % (result[3]))
            headers.append("Message-ID: " + self.get_message_id(result[0], group_name))
            headers.append("Xref: %s %s:%s" % (settings.nntp_hostname, group_name, result[0]))
            if result[6] != 0:
                headers.append("References: " + self.get_message_id(result[6], group_name))
                headers.append("In-Reply-To: " + self.get_message_id(result[6], group_name))
        if headers_only:
            return "\r\n".join(headers)
        if body_only:
            return strutil.format_body(result[5])
        return ("\r\n".join(headers), strutil.format_body(result[5]))

    def get_LAST(self, group_name, current_id):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    ID
                FROM
                    %s
                WHERE
                    post_type='post' AND post_status='publish' AND
                    ID < %s
                ORDER BY
                    ID DESC
                LIMIT 0, 1""" % (table_name, current_id)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        return self.cursor.fetchone()[0]

    def get_NEXT(self, group_name, current_id):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    ID
                FROM
                    %s
                WHERE
                    post_type='post' AND post_status='publish' AND
                    ID > %s
                ORDER BY
                    ID ASC
                LIMIT 0, 1""" % (table_name, current_id)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        return self.cursor.fetchone()[0]

    def get_HEAD(self, group_name, id):
        return self.get_ARTICLE(group_name, id, headers_only=True)

    def get_BODY(self, group_name, id):
        return self.get_ARTICLE(group_name, id, body_only=True)

    def get_XOVER(self, group_name, start_id, end_id='ggg'):
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
                    A.post_author=wp_users.ID AND
                    A.ID >= %s""" % (table_name, start_id)
        if end_id != 'ggg':
            stmt = "%s AND A.ID <= %s" % (stmt, end_id)
        self.cursor.execute(stmt)
        result = list(self.cursor.fetchall())
        overviews = []
        for row in result:
            if row[3] == '':
                author = row[2]
            else:
                author = "%s <%s>" % (row[2], row[3])
            formatted_time = strutil.get_formatted_time(time.localtime(row[5]))
            message_id = self.get_message_id(row[0], group_name)
            line_count = len(row[6].split('\n'))
            xref = 'Xref: %s %s:%s' % (settings.nntp_hostname, group_name, row[0])
            if row[1] != 0:
                reference = self.get_message_id(row[1], group_name)
            else:
                reference = ""
            # message_number <tab> subject <tab> author <tab> date <tab> message_id <tab> reference <tab> bytes <tab> lines <tab> xref
            overviews.append("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (row[0], row[4], author, formatted_time, message_id, reference, len(strutil.format_body(row[6])), line_count, xref))
        return "\r\n".join(overviews)

    def get_XPAT(self, group_name, header, pattern, start_id, end_id='ggg'):
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
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    ID
                FROM
                    %s
                WHERE
                    post_type='post' AND post_status='publish'
                ORDER BY
                    id ASC""" % (table_name)
        self.cursor.execute(stmt)
        result = list(self.cursor.fetchall())
        return "\r\n".join(["%s" % k for k in result])

    def get_XGTITLE(self, pattern=None):
        return "blog.singpolyma Singpolyma" # TODO

    def get_XHDR(self, group_name, header, style, range):
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
                    post_author = wp_users.ID AND """ % (table_name)
        if style == 'range':
            stmt = '%s A.id >= %s' % (stmt, range[0])
            if len(range) == 2:
                stmt = '%s AND A.id <= %s' % (stmt, range[1])
        else:
            stmt = '%s A.id = %s' % (stmt, range[0])
        if self.cursor.execute(stmt) == 0:
            return None
        result = self.cursor.fetchall()
        hdrs = []
        for row in result:
            if header.upper() == 'SUBJECT':
                hdrs.append('%s %s' % (row[0], row[4]))
            elif header.upper() == 'FROM':
                hdrs.append('%s %s <%s>' % (row[0], row[2], row[3]))
            elif header.upper() == 'DATE':
                hdrs.append('%s %s' % (row[0], strutil.get_formatted_time(time.localtime(result[5]))))
            elif header.upper() == 'MESSAGE-ID':
                hdrs.append(row[0] + ' ' + self.get_message_id(row[0], group_name))
            elif (header.upper() == 'REFERENCES') and (row[1] != 0):
                hdrs.append(row[0] + ' ' + self.get_message_id(row[1], group_name))
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
