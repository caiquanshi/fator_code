import unittest
from unittest.mock import patch, MagicMock, call
from classes import *
from twitch import *
DATABASE = 'twitch.db'

class MockArgsParser:
    def __init__(self, name, channel_id, stream_id, file, filter_string):
        self.name = name
        self.id = channel_id
        self.channel_id = channel_id
        self.stream_id = stream_id
        self.file = file
        self.filters = filter_string


class TestChatLogMethods(unittest.TestCase):
    def setUp(self):
        self.channel_id = '137512364'
        self.channel_name = 'OverwatchLeague'

    def testCreateChannelInit(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect = MagicMock()
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            create_channel = CreateChannel(DATABASE)
            self.assertEqual(mock_db.connect().cursor.call_count, 2)

    def test_create_channel(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect().cursor().execute = MagicMock()
            mock_db.connect().commit = MagicMock()
            create_channel = CreateChannel(DATABASE)
            create_table_query = 'CREATE TABLE if not exists channels ' \
                                 '(channel_id integer primary key, channel_name text)'
            insert_query = "INSERT INTO CHANNELS VALUES ({},'{}')". \
                format(self.channel_id, self.channel_name)
            expected_calls = [
                call(create_table_query),
                call(insert_query)
            ]
            create_channel.create_channel(self.channel_id, self.channel_name)
            mock_db.connect().cursor().execute.assert_has_calls(expected_calls)
            self.assertEqual(mock_db.connect().commit.call_count, 1)
    
    def test_create_channel_exception(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect().cursor().execute = MagicMock(side_effect= Exception())
            mock_db.connect().commit = MagicMock()
            create_channel = CreateChannel(DATABASE)
            create_channel.create_channel(self.channel_id, self.channel_name)
            self.assertEqual(mock_db.connect().close.call_count, 1)
            self.assertEqual(mock_db.connect().commit.call_count, 0)

    def test_log_all_information_for_table(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect().cursor().execute = MagicMock(return_value=[(137512364, 'OverwatchLeague')])
            create_channel = CreateChannel(DATABASE)
            channel_query = "select * from channels where channel_id = {}" \
                .format(self.channel_id)
            expected_calls = [
                call(channel_query)
            ]
            create_channel.create_channel(self.channel_id, self.channel_name)
            result = create_channel.log_all_infomation_for_table(self.channel_id)
            mock_db.connect().cursor().execute.assert_has_calls(expected_calls)
            self.assertEqual(result, (137512364, 'OverwatchLeague'))
    
    def test_log_all_information_for_table_exception(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect().cursor().execute = MagicMock()
            create_channel = CreateChannel(DATABASE)
            create_channel.create_channel(self.channel_id, self.channel_name)
            mock_db.connect().cursor().execute = MagicMock(side_effect= Exception())
            result = create_channel.log_all_infomation_for_table(self.channel_id)
            self.assertEqual(mock_db.connect().close.call_count, 1)
            self.assertIsNone(result)


class TestParseTopSpam(unittest.TestCase):
    def setUp(self):
        self.file_name ='test.json'
    def testParseTopSpamInit(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect = MagicMock()
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            parse_top_spam = ParseTopSpam(DATABASE, self.file_name)
            self.assertEqual(mock_db.connect().cursor.call_count, 2)

    def testGetBasicInfo(self):
        info = get_basic_info_from_file_parse(self.file_name)
        comment_to_user = {"PLEASE BE A FAIR MATCH, to get more hours of tokens": {"seaskythe"},
                          "reward A Cheer shared Rewards to 10 others in Chat!": {"InfiniteSoulOW"},
                          "this will be close, for sure.": {"thesecondusername"},
                          "ooo": {"Mai_Creationz"}}
        comment_count = {"PLEASE BE A FAIR MATCH, to get more hours of tokens": 12,
                          "reward A Cheer shared Rewards to 10 others in Chat!": 1,
                          "this will be close, for sure.": 1,
                          "ooo": 1}
        self.assertEqual(info['comment_count'], comment_count)
        self.assertEqual(info['comment_to_users'], comment_to_user)
        self.assertEqual(info['channel_id'], "137512364")
        self.assertEqual(info['stream_id'], "451603129")
    

    def test_delete_original_spam_info(self):
        info = get_basic_info_from_file_parse(self.file_name)
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            parse_top_spam = ParseTopSpam(DATABASE, self.file_name)
            parse_top_spam.delete_original_spam_info(info['channel_id'], info['stream_id'])
            set_foreign_keys = 'PRAGMA foreign_keys = ON;'
            create_table_query = 'create table if not exists top_spam' \
                                 ' (channel_id integer NOT NULL, stream_id' \
                                 ' integer NOT NULL, spam_text string, spam' \
                                 '_occurrences integer,' \
                                 ' spam_user_count integer, ' \
                                 'FOREIGN KEY(channel_id)' \
                                 ' REFERENCES channels(channel_id))'
            delete_original_row = 'delete from top_spam where channel_id ' \
                                  '= {} and stream_id = {}'. \
                format(info['channel_id'], info['stream_id'])
            expected_calls = [
                call(set_foreign_keys),
                call(create_table_query),
                call(delete_original_row)
            ]
            mock_db.connect().cursor().execute.assert_has_calls(expected_calls)
            self.assertEqual(mock_db.connect().commit.call_count, 1)
    
    def test_delete_original_spam_info_exception(self):
        info = get_basic_info_from_file_parse(self.file_name)
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock(side_effect= Exception())
            parse_top_spam = ParseTopSpam(DATABASE, self.file_name)
            parse_top_spam.delete_original_spam_info(info['channel_id'], info['stream_id'])
            self.assertEqual(mock_db.connect().close.call_count, 1)
            self.assertEqual(mock_db.connect().commit.call_count, 0)

    def test_insert_new_spam_row(self):
        info = get_basic_info_from_file_parse(self.file_name)
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            parse_top_spam = ParseTopSpam(DATABASE, self.file_name)
            parse_top_spam.delete_original_spam_info(info['channel_id'], info['stream_id'])
            result = parse_top_spam.insert_new_spam_row(info)
            self.assertEqual(mock_db.connect().commit.call_count, 2)
            self.assertEqual(mock_db.connect().execute.call_count, 0)
            parse_info = "inserted {} top spam records for stream {} " \
                         "on channel {}".format('1', info['stream_id'], info['channel_id'])
            self.assertEqual(result, parse_info)

    def test_insert_new_spam_row_exception(self):
        info = get_basic_info_from_file_parse(self.file_name)
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            parse_top_spam = ParseTopSpam(DATABASE, self.file_name)
            parse_top_spam.delete_original_spam_info(info['channel_id'], info['stream_id'])
            mock_db.connect().cursor().execute = MagicMock(side_effect= Exception())
            result = parse_top_spam.insert_new_spam_row(info)
            self.assertEqual(mock_db.connect().close.call_count, 1)
            self.assertEqual(mock_db.connect().commit.call_count, 1)
            self.assertIsNone(result)


class TestGetTopSpam(unittest.TestCase):
    def setUp(self):
        self.channel_id = '137512364'
        self.stream_id = '451603129'

    def testGetTopSpamInit(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect = MagicMock()
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            get_top_spam = GetTopSpam(DATABASE)
            self.assertEqual(mock_db.connect().cursor.call_count, 2)

    def testGetTopSpam(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect().commit = MagicMock()
            mock_return = [(0, 0, 'text1', 10, 10), (0, 0, 'text2', 11, 11)]
            mock_db.connect().cursor().execute = MagicMock(return_value= mock_return)
            get_top_spam = GetTopSpam(DATABASE)
            result = get_top_spam.get_spam(self.channel_id, self.stream_id)
            get_spam_query = "select * from top_spam where channel_id = {}" \
                             " and stream_id = {} order by spam_occurrences desc," \
                             " spam_user_count desc, spam_text". \
                format(self.channel_id, self.stream_id)
            expected_calls = [
                call(get_spam_query)
            ]
            mock_db.connect().cursor().execute.assert_has_calls(expected_calls)
            expected_result = [{"occurrences": 10, "spam_text": "text1", "user_count": 10},
                               {"occurrences": 11, "spam_text": "text2",
                                "user_count": 11}]
            expected_result = json.dumps(expected_result, sort_keys=True)
            self.assertEqual(expected_result, result)

    def testGetTopSpam_exception(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect().commit = MagicMock()
            mock_return = [(0, 0, 'text1', 10, 10), (0, 0, 'text2', 11, 11)]
            mock_db.connect().cursor().execute = MagicMock(side_effect= Exception())
            get_top_spam = GetTopSpam(DATABASE)
            result = get_top_spam.get_spam(self.channel_id, self.stream_id)
            self.assertEqual(mock_db.connect().close.call_count, 1)
            self.assertIsNone(result)


class TestStoreChatLog(unittest.TestCase):
    def setUp(self):
        self.file_name ='test.json'
        self.stream_id = '451603129'
        self.channel_id = '137512364'
    def test_init(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect = MagicMock()
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            store_chat_log = StoreChatLog(DATABASE)
            self.assertIsNone(store_chat_log.channel_id)
            self.assertIsNone(store_chat_log.stream_id)
            self.assertIsNone(store_chat_log.comments)

    def test_get_basic_info(self):
        info = get_basic_info_from_file_store_log(self.file_name)

        self.assertEqual(info['channel_id'], self.channel_id)
        self.assertEqual(info['stream_id'], self.stream_id)

    def test_create_tabel(self):
        info = get_basic_info_from_file_store_log(self.file_name)
        with patch('classes.sqlite3') as mock_db:
            channel_id = info['channel_id']
            stream_id = info['stream_id']
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            store_chat_log = StoreChatLog(DATABASE)
            store_chat_log.create_table(info)
            create_table_query = 'create table if not exists chat_log ' \
                             '(channel_id integer NOT NULL, ' \
                             'stream_id integer NOT NULL, text string,' \
                             ' user string, chat_time datetime, offset int, ' \
                             'FOREIGN KEY(channel_id)' \
                             ' REFERENCES channels(channel_id))'
            delete_row_query = \
                'delete from chat_log where channel_id ={} and stream_id ={}' \
                    .format(channel_id, stream_id)
            expected_calls = [
                call(create_table_query),
                call(delete_row_query)
            ]
            mock_db.connect().cursor().execute.assert_has_calls(expected_calls)
            self.assertEqual(mock_db.connect().commit.call_count, 1)

    def test_create_tabel_exception(self):
        info = get_basic_info_from_file_store_log(self.file_name)
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock(side_effect= Exception())
            store_chat_log = StoreChatLog(DATABASE)
            store_chat_log.create_table(info)
            self.assertEqual(mock_db.connect().close.call_count, 1)
            self.assertEqual(mock_db.connect().commit.call_count, 0)

    def test_insert_log(self):
        info = get_basic_info_from_file_store_log(self.file_name)
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect = MagicMock()
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            store_chat_log = StoreChatLog(DATABASE)
            store_chat_log.create_table(info)
            result = store_chat_log.insert_log(info)
            expected_result = "inserted {} records to chat log for " \
                      "stream {} on channel {}" \
            .format(15, self.stream_id, self.channel_id)
            self.assertEqual(expected_result, result)

    def test_insert_log_exception(self):
        info = get_basic_info_from_file_store_log(self.file_name)
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect = MagicMock()
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            store_chat_log = StoreChatLog(DATABASE)
            store_chat_log.create_table(info)
            mock_db.connect().cursor().execute = MagicMock(side_effect= Exception())
            result = store_chat_log.insert_log(info)
            self.assertEqual(mock_db.connect().close.call_count, 1)
            self.assertEqual(mock_db.connect().commit.call_count, 1)
            self.assertIsNone(result)

class TestQueryChatLog(unittest.TestCase):
    def setUp(self):
        self.filter_string = ["stream_id eq 497295395", "user eq Moobot"]

    def test_init(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect = MagicMock()
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            query_chat_log = QueryChatLog(DATABASE)
            self.assertEqual(query_chat_log.query_string, "select * from chat_log ")
            self.assertEqual(mock_db.connect().cursor.call_count, 2)

    def test_make_query_string(self):
        query_chat_log = QueryChatLog(DATABASE)
        query_chat_log.make_query_string(self.filter_string)
        expected_query_string = "select * from chat_log where stream_id = " \
                                "497295395 AND user = 'Moobot'  order by chat_time"
        self.assertEqual(expected_query_string, query_chat_log.query_string)

    def test_do_query(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect = MagicMock()
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock()
            query_chat_log = QueryChatLog(DATABASE)
            query_chat_log.make_query_string(self.filter_string)
            query_chat_log.do_query()
            query_string = "select * from chat_log where stream_id = " \
                                "497295395 AND user = 'Moobot'  order by chat_time"
            expected_calls = [
                call(query_string)
            ]
            mock_db.connect().cursor().execute.assert_has_calls(expected_calls)

    def test_do_query_exception(self):
        with patch('classes.sqlite3') as mock_db:
            mock_db.connect = MagicMock()
            mock_db.connect().cursor = MagicMock()
            mock_db.connect().close = MagicMock()
            mock_db.connect().commit = MagicMock()
            mock_db.connect().cursor().execute = MagicMock(side_effect= Exception())
            query_chat_log = QueryChatLog(DATABASE)
            query_chat_log.make_query_string(self.filter_string)
            query_chat_log.do_query()
            self.assertEqual(mock_db.connect().close.call_count, 1)

if __name__ == '__main__':
    unittest.main()
