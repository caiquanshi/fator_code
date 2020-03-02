import json
import sys
import argparse
import sqlite3
import logging
from classes import *

DATABASE = 'twitch.db'

def get_basic_info_from_file_parse(file):
        """Store information needed for file to DAO class attributes"""
        comment_count = {}
        comment_to_users = {}
        result = {}
        with open(file) as comments_file:
            comments = json.load(comments_file)['comments']
            try:
                result['channel_id'] = comments[0]["channel_id"]
                result['stream_id'] = comments[0]["content_id"]
            except IndexError:
                logging.error('file is empty')
                return {}
            for comment in comments:
                user = comment["commenter"]["display_name"]
                message_body = comment["message"]["body"]
                if message_body in comment_count:
                    comment_count[message_body] += 1
                else:
                    comment_count[message_body] = 1
                if message_body in comment_to_users:
                    comment_to_users[message_body].add(user)
                else:
                    comment_to_users[message_body] = set()
                    comment_to_users[message_body].add(user)
        result['comment_count'] = comment_count
        result['comment_to_users'] = comment_to_users
        return result

def get_basic_info_from_file_store_log(file_name):
        """Store basic information from file to DAO"""
        result = {}
        with open(file_name) as file:
            formatted_file = json.load(file)

            comments = formatted_file['comments']
            result['comments'] = comments
            try:
                result['channel_id'] = comments[0]["channel_id"]
                result['stream_id'] = comments[0]["content_id"]
            except IndexError:
                logging.error('file is empty')
                return {}
        return result

def argparse_init():

    """Initialize the argument parser"""
    argument_parser = argparse.ArgumentParser(description='Parse Twitch chatlogs')
    sub_parser = argument_parser.add_subparsers(dest='sub')
    parser_create_channel = sub_parser.add_parser('createchannel')
    parser_create_channel.add_argument('name')
    parser_create_channel.add_argument('id', type=int)

    parser_parse_spam = sub_parser.add_parser('parsetopspam')
    parser_parse_spam.add_argument('file')

    parser_get_spam = sub_parser.add_parser('gettopspam')
    parser_get_spam.add_argument("channel_id")
    parser_get_spam.add_argument("stream_id")

    parser_get_spam2 = sub_parser.add_parser('gettopspam2')
    parser_get_spam2.add_argument("channel_id")
    parser_get_spam2.add_argument("stream_id")

    parser_get_viewership = sub_parser.add_parser('viewership')
    parser_get_viewership.add_argument("channel_id")
    parser_get_viewership.add_argument("stream_id")

    parser_store_chat_log = sub_parser.add_parser("storechatlog")
    parser_store_chat_log.add_argument('file')

    parser_query_chat_log = sub_parser.add_parser("querychatlog")
    parser_query_chat_log.add_argument("filters", nargs="+")

    return argument_parser.parse_args()


def main():

    parse_args = argparse_init()

    if parse_args.sub == "createchannel":
        channel_name = parse_args.name
        channel_id = parse_args.id
        create_table = CreateChannel(DATABASE)
        create_table.create_channel(channel_id, channel_name)
        print(create_table.log_all_infomation_for_table(channel_id))
        create_table.disconnect()

    elif parse_args.sub == "parsetopspam":
        file_name = parse_args.file
        info = get_basic_info_from_file_parse(file_name)
        parse_top_spam = ParseTopSpam(DATABASE, file_name)
        parse_top_spam.delete_original_spam_info(info['channel_id'], info['stream_id'])
        print(parse_top_spam.insert_new_spam_row(info))
        parse_top_spam.disconnect()

    elif parse_args.sub == "gettopspam":
        channel_id = parse_args.channel_id
        stream_id = parse_args.stream_id
        get_top_spam = GetTopSpam(DATABASE)
        print(get_top_spam.get_spam(channel_id, stream_id))
        get_top_spam.disconnect()

    elif parse_args.sub == "storechatlog":
        file_name = parse_args.file
        info = get_basic_info_from_file_store_log(file_name)
        store_chat_log = StoreChatLog(DATABASE)
        store_chat_log.create_table(info)
        print(store_chat_log.insert_log(info))
        store_chat_log.disconnect()

    elif parse_args.sub == 'querychatlog':
        filter_string = parse_args.filters
        query_chat_log = QueryChatLog(DATABASE)
        query_chat_log.make_query_string(filter_string)
        print(query_chat_log.do_query())
        query_chat_log.disconnect()

    elif parse_args.sub == "gettopspam2":
        channel_id = parse_args.channel_id
        stream_id = parse_args.stream_id
        get_top_spam = GetTopSpam2(DATABASE)
        print(get_top_spam.get_spam(channel_id, stream_id))
        get_top_spam.disconnect()

    elif parse_args.sub == "viewership":
        channel_id = parse_args.channel_id
        stream_id = parse_args.stream_id
        viewership = ViewershipMetrics(DATABASE)
        chat_logs = viewership.query_viewers(channel_id, stream_id)
        viewership.disconnect()
        print(viewership.make_viewer_data(chat_logs, channel_id, stream_id))


if __name__ == "__main__":
    main()
