import json
import sys
import sqlite3
import logging


class DAO:
    """A date access object super class """

    def __init__(self, database):
        self.connection = sqlite3.connect(database)
        self.queryCursor = self.connection.cursor()
        logging.basicConfig(level=logging.ERROR)
        self.logger = logging.getLogger(__name__)

    def disconnect(self):
        """Disconnect form data base"""
        self.connection.close()

    def commit(self):
        """Commit changes to date base """
        self.connection.commit()


class CreateChannel(DAO):
    """ A date access object class for create channl"""

    def __init__(self, database):
        DAO.__init__(self, database)
        self.logger.debug('creating channel')

    def create_channel(self, channel_id, channel_name):
        """Create a chanel table to date base"""
        create_table_query = 'CREATE TABLE if not exists channels ' \
                             '(channel_id integer primary key, ' \
                             'channel_name text)'
        insert_query = "INSERT INTO CHANNELS VALUES ({},'{}')". \
            format(channel_id, channel_name)
        try:
            self.queryCursor.execute(create_table_query)
            self.queryCursor.execute(insert_query)
        except Exception as e:
            self.logger.error('Failed to create channels table: ' + str(e))
            self.disconnect()
            return
        self.commit()


    def log_all_infomation_for_table(self, channel_id):
        """ log all rows in table"""
        channel_query = "select * from channels where channel_id = {}" \
            .format(channel_id)
        try:
            rows = self.queryCursor.execute(channel_query)
        except Exception as e:
            self.logger.error('Failed to create channels table: ' + str(e))
            self.disconnect()
            return
        for row in rows:
            self.logger.info(row)
            name = str(row[1])
            return (row[0], name)


class ParseTopSpam(DAO):
    """ A date access object class for parsing top spam"""

    def __init__(self, database, file_name):
        DAO.__init__(self, database)
        self.logger.debug("generating summary for twitch chat log in {}"
                          .format(file_name))

    def delete_original_spam_info(self,channel_id, stream_id):
        """Delete the original spam row for table"""
        set_foreign_keys = 'PRAGMA foreign_keys = ON;'
        create_table_query = 'create table if not exists top_spam' \
                             ' (channel_id integer NOT NULL, ' \
                             'stream_id integer NOT NULL, spam_text string,' \
                             ' spam_occurrences integer,' \
                             ' spam_user_count integer,' \
                             ' FOREIGN KEY(channel_id)' \
                             ' REFERENCES channels(channel_id))'
        delete_original_row = 'delete from top_spam where channel_id ' \
                              '= {} and stream_id = {}'. \
            format(channel_id, stream_id)
        try:
            self.queryCursor.execute(set_foreign_keys)
            self.queryCursor.execute(create_table_query)
            self.queryCursor.execute(delete_original_row)
        except Exception as e:
            self.logger.error('Failed to set creat tabel: ' + str(e))
            self.disconnect()
            return
        self.commit()

    def insert_new_spam_row(self, info):
        """Insert new spam row to table"""
        comment_count = info['comment_count']
        channel_id = info['channel_id']
        stream_id = info['stream_id']
        comment_to_users = info['comment_to_users']
        sorted_counts = sorted(comment_count.items(),
                               key=lambda kv: kv[1], reverse=True)
        num_insertions = 0
        for i, (comment, numComments) in enumerate(sorted_counts):
            user_count = len(comment_to_users[comment])
            self.logger.debug(i, comment, numComments, user_count)
            # should probably parameterize "10"
            if numComments > 10:
                try:
                    self.queryCursor.execute('insert into top_spam values'
                                             '(?,?,?,?,?)',
                                             (channel_id, stream_id,
                                              comment, numComments, user_count))
                except Exception as e:
                    self.logger.error('Failed to insert spam row: ' + str(e))
                    self.disconnect()
                    return
                num_insertions += 1
        self.commit()
        self.logger.info("inserted {} top spam records for stream {} "
                         "on channel {}".format(num_insertions, stream_id,
                                                channel_id))
        parse_info = "inserted {} top spam records for stream {} " \
                     "on channel {}".format(num_insertions, stream_id,
                                            channel_id)

        return parse_info


class GetTopSpam(DAO):
    """A data access object for get top spam"""

    def __init__(self, database):
        DAO.__init__(self, database)
        self.logger.debug("getting top spam")

    def get_spam(self, channel_id, stream_id):
        """Query top spam table and log it by json format"""
        result = []
        get_spam_query = "select * from top_spam where channel_id = {}" \
                         " and stream_id = {} order by spam_occurrences desc," \
                         " spam_user_count desc, spam_text". \
            format(channel_id, stream_id)
        try:
            rows = self.queryCursor.execute(get_spam_query)
        except Exception as e:
            self.logger.error('Failed to get top spam: ' + str(e))
            self.disconnect()
            return
        for row in rows:
            result.append({"spam_text": row[2],
                           "occurrences": row[3], "user_count": row[4]})
        self.logger.info(json.dumps(result, sort_keys=True))
        return json.dumps(result, sort_keys=True)


class GetTopSpam2(DAO):
    """A data access object for get top spam"""

    def __init__(self, database):
        DAO.__init__(self, database)
        self.logger.debug("getting top spam")

    def get_spam(self, channel_id, stream_id):
        """Query top spam table and log it by json format"""
        result = []
        comment_count = {}
        comment_to_users = {}
        get_spam_query = "select * from chat_log where channel_id = {}" \
                         " and stream_id = {} ". \
            format(channel_id, stream_id)
        try:
            rows = self.queryCursor.execute(get_spam_query)
        except Exception as e:
            self.logger.error('Failed to get top spam: ' + str(e))
            self.disconnect()
            return

        for row in rows:
            user = row[3]
            message_body = row[2]
            logging.debug(message_body)
            if message_body in comment_count:
                comment_count[message_body] += 1
            else:
                comment_count[message_body] = 1
            if message_body in comment_to_users:
                comment_to_users[message_body].add(user)
            else:
                comment_to_users[message_body] = set()
                comment_to_users[message_body].add(user)

        for comment in comment_count:
            if comment_count[comment] > 10:
                result.append({"occurrences": comment_count[comment], "spam_text": comment,
                               "user_count": len(comment_to_users[comment])})
        # the result didn't order by spam_occurrences desc, spam_user_count desc, spam_text
        result = sorted(result, key=lambda i: i["spam_text"])
        result = sorted(result, key=lambda i: (i["occurrences"], i["user_count"]), reverse=True)
        return json.dumps(result, sort_keys=True)


class StoreChatLog(DAO):
    """A data access object for storing chat log"""

    def __init__(self, database):
        DAO.__init__(self, database)
        self.channel_id = None
        self.stream_id = None
        self.comments = None
        self.logger.debug("storing raw logs in table")

    def create_table(self, info):
        channel_id = info['channel_id']
        stream_id = info['stream_id']
        create_table_query = 'create table if not exists chat_log ' \
                             '(channel_id integer NOT NULL, ' \
                             'stream_id integer NOT NULL, text string,' \
                             ' user string, chat_time datetime, offset int, ' \
                             'FOREIGN KEY(channel_id)' \
                             ' REFERENCES channels(channel_id))'
        delete_row_query = \
            'delete from chat_log where channel_id ={} and stream_id ={}' \
                .format(channel_id, stream_id)
        try:
            self.queryCursor.execute(create_table_query)
            self.queryCursor.execute(delete_row_query)
        except Exception as e:
            self.logger.error('Failed to create chat log table: ' + str(e))
            self.disconnect()
            return
        self.commit()

    def insert_log(self, info):
        channel_id = info['channel_id']
        stream_id = info['stream_id']
        comments = info['comments']
        for comment in comments:
            try:
                self.queryCursor.execute("insert into chat_log VALUES"
                                         " (?,?,?,?,?,?)",
                                         (channel_id, stream_id,
                                          comment["message"]["body"],
                                          comment["commenter"]["display_name"],
                                          comment["created_at"],
                                          comment["content_offset_seconds"]))
            except Exception as e:
                self.logger.error('Failed to insert chat log: ' + str(e))
                self.disconnect()
                return
        self.commit()
        insert_info = "inserted {} records to chat log for " \
                      "stream {} on channel {}" \
            .format(len(comments), stream_id, channel_id)
        return insert_info


class QueryChatLog(DAO):
    """A data access object for querying chat log """

    def __init__(self, database):
        DAO.__init__(self, database)
        self.query_string = "select * from chat_log "
        self.logger.debug("query chat log")

    def make_query_string(self, filter_string):
        """Preparing query string by given filters in args"""
        if len(filter_string) > 0:
            self.query_string += "where "
        for filter in filter_string:
            # first element be column name, second be operator, last be the variable
            parameters = filter.split(' ')

            self.query_string += parameters[0]
            self.convert_oprater(parameters[1])
            self.str_variable(parameters[2], parameters[0])

        if len(filter_string) > 0:
            self.query_string = self.query_string[:-4]

        self.query_string += " order by chat_time"

    def convert_oprater(self, operator):
        convert_dict = {"eq": " = ", "gt": " > ", "lt":" < ", "gteq":" >= ", "lteq": " <= ", "like":" like "}
        self.query_string += convert_dict[operator]

    def str_variable(self, condition_variable, column):
        if column in ['text', 'user']:
            self.query_string += "'" + condition_variable + "' AND "
        else:
            self.query_string += condition_variable + ' AND '

    def do_query(self):
        """executing prepared query string"""
        out = []
        try:
            rows = self.queryCursor.execute(self.query_string)
        except Exception as e:
            self.logger.error('Failed to query chat log table: ' + str(e))
            self.disconnect()
            return
        names = []
        for d in rows.description:
            names.append(d[0])
        for row in rows:
            self.logger.debug(row)
            json_formatted_row = {}
            for i in range(len(names)):
                json_formatted_row[names[i]] = row[i]
            out.append(json_formatted_row)
        self.logger.info(json.dumps(out, sort_keys=True))
        return json.dumps(out, sort_keys=True)


class ViewershipMetrics(DAO):
    """A data access object for querying chat log """

    def __init__(self, database):
        DAO.__init__(self, database)

    def query_viewers(self, channel_id, stream_id):
        chat_logs = []
        get_chat_log_query = "select * from chat_log where channel_id = {}" \
                             " and stream_id = {} order by chat_time" \
            .format(channel_id, stream_id)
        try:
            rows = self.queryCursor.execute(get_chat_log_query)
        except Exception as e:
            self.logger.error('Failed to get top spam: ' + str(e))
            self.disconnect()
            return

        for row in rows:
            chat_logs.append(row)
        return chat_logs

    def make_viewer_data(self, chat_logs, channel_id, stream_id):
        if len(chat_logs) == 0:
            self.logger.info('no data')
            return json.dumps([], sort_keys=True)
        offset_dict = {}
        start_time = chat_logs[0][4]
        for row in chat_logs:
            user = row[3]
            offset = (row[5] // 60) + 1
            if offset in offset_dict:
                offset_dict[offset]['messages'] += 1
                offset_dict[offset]['viewers'].add(user)
            else:
                offset_dict[offset] = {'messages': 1, 'viewers': set()}
                offset_dict[offset]['viewers'].add(user)

        result = {"channel_id": channel_id, 'stream_id': stream_id,
                  "starttime": start_time, "per_minute": []}
        for offset in offset_dict:
            per_minute_tamp = {"offset": offset, "viewers": len(offset_dict[offset]["viewers"]),
                               "messages": offset_dict[offset]["messages"]}
            result["per_minute"].append(per_minute_tamp)
        return json.dumps([result], sort_keys=True)
