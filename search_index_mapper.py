#!/usr/bin/python
"""
Emits the following (key,value) pair: <word> <node_type> <id> for each
word contained in a forum node body fields

http://www.michael-noll.com/tutorials/
    writing-an-hadoop-mapreduce-program-in-python/
"""
import csv
import re
import sys

def parse_forum_node(fields):
    """Creates a dictionary that stores a forum node entry

    Args:
        fields: List that stores the elements of a forum node entry

    Returns:
        forum_node: dictionary that stores a forum node entry"""
    field_names = ['id',\
                   'title',\
                   'tagnames',\
                   'author_id',\
                   'body',\
                   'node_type',\
                   'parent_id',\
                   'abs_parent_id',\
                   'added_at',\
                   'score',\
                   'state_string',\
                   'last_edited_id',\
                   'last_activity_by_id',\
                   'last_activity_at',\
                   'active_revision_id',\
                   'extra',\
                   'extra_ref_id',\
                   'extra_count',\
                   'marked']

    forum_node = {}

    for idx in range(0, len(fields)):
        forum_node[field_names[idx]] = fields[idx]

    # http://stackoverflow.com/questions/1798465/
    #     python-remove-last-3-characters-of-a-string
    forum_node['added_at'] = forum_node['added_at'][0:-3]

    forum_node['tagnames'] = forum_node['tagnames'].rstrip()

    return forum_node

def initialize_stopword_list():
    """Initializes a "list of common english words" which are also referred
    to as "stopwords"

    Reference:
    ---------
    http://www.textfixer.com/resources/
        common-english-words-with-contractions.txt

    Args:
        None

    Returns:
        stopwords: List that contains "common english words" """
    stopwords = ["'tis", "'twas", "a", "able", "about", "across", "after",
                 "ain't", "all", "almost", "also", "am", "among",
                 "an", "and", "any", "are", "aren't", "as",
                 "at", "be", "because", "been", "but", "by",
                 "can", "can't", "cannot", "could", "could've", "couldn't",
                 "dear", "did", "didn't", "do", "does", "doesn't",
                 "don't", "either", "else", "ever", "every", "for",
                 "from", "get", "got", "had", "has", "hasn't",
                 "have", "he", "he'd", "he'll", "he's", "her",
                 "hers", "him", "his", "how", "how'd", "how'll",
                 "how's", "however", "i", "i'd", "i'll", "i'm",
                 "i've", "if", "in", "into", "is", "isn't",
                 "it", "it's", "its", "just", "least", "let",
                 "like", "likely", "may", "me", "might", "might've",
                 "mightn't", "most", "must", "must've", "mustn't", "my",
                 "neither", "no", "nor", "not", "of", "off",
                 "often", "on", "only", "or", "other", "our",
                 "own", "rather", "said", "say", "says", "shan't",
                 "she", "she'd", "she'll", "she's", "should", "should've",
                 "shouldn't", "since", "so", "some", "than", "that",
                 "that'll", "that's", "the", "their", "them", "then",
                 "there", "there's", "these", "they", "they'd", "they'll",
                 "they're", "they've", "this", "tis", "to", "too",
                 "twas", "us", "wants", "was", "wasn't", "we",
                 "we'd", "we'll", "we're", "were", "weren't", "what",
                 "what'd", "what's", "when", "when", "when'd", "when'll",
                 "when's", "where", "where'd", "where'll", "where's", "which",
                 "while", "who", "who'd", "who'll", "who's", "whom",
                 "why", "why'd", "why'll", "why's", "will", "with",
                 "won't", "would", "would've", "wouldn't", "yet", "you",
                 "you'd", "you'll", "you're", "you've", "your", "s", "x",
                 "com", "def", "e", "py"]

    return stopwords

def mapper(stdin):
    """Emits the following (key,value) pair: <word> <node_type> <id> for each
    word contained in a forum node body field

    Args:
        stdin: Standard input file handle

    Returns:
        None"""
    split_re = "[\\s.,!\\?:;\"\\(\\)<>\\[\\]#\\$=\\-/]"

    # http://www.w3schools.com/TAgs/att_a_rel.asp
    # http://www.w3schools.com/tags/tag_li.asp
    # http://www.w3schools.com/htmL/html_entities.asp
    # http://www.w3schools.com/tags/tag_body.asp
    # http://www.tei-c.org/release/doc/tei-p5-doc/en/html/
    #   examples-hi.html
    # http://www.w3schools.com/jsref/dom_obj_fileupload.asp
    # http://www.w3schools.com/TAgs/att_a_rel.asp
    # http://www.w3schools.com/htmL/html_head.asp
    html_tags = ['p', 'href', 'li', 'ul', 'rel', '&lt', 'br', 'body', 'html',
                 'http', 'https', 'hi', 'file', 'nofollow', 'www', 'com',
                 'head']

    stopwords = initialize_stopword_list()

    for fields in csv.reader(stdin, delimiter="\t"):
        forum_node = parse_forum_node(fields)

        for cur_word in re.split(split_re, forum_node['body']):
            if len(cur_word) > 0:
                # Transform word to lowercase
                cur_word = cur_word.lower()

                # Remove quotations mark(s) from word
                cur_word = re.sub("^[']*([a-z0-9]*)[']*$", "\\1", cur_word)

                # Remove HTML entities
                # http://www.w3schools.com/htmL/html_entities.asp
                cur_word = re.sub("&[l|g]t", "", cur_word)

                # Set image size (e.g. 400px) word to the empty string
                cur_word = re.sub("([0-9]+px)", "", cur_word)

                # Remove HTML form
                # http://www.w3schools.com/htmL/html_forms.asp
                cur_word = re.sub("^form.*$", "", cur_word)

                # Remove HTML heading
                # http://www.w3schools.com/tags/tag_hn.asp
                cur_word = re.sub("^h[0-9]$", "", cur_word)

                valid_word = len(cur_word) > 0
                valid_word = valid_word and cur_word not in html_tags
                valid_word = valid_word and cur_word not in stopwords

                if valid_word:
                    print "%s\t%s" % (cur_word, forum_node['id'])

if __name__ == "__main__":
    mapper(sys.stdin)
