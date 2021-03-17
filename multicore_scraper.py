import multiprocessing
from ccl_scratch_tools import Scraper
import pandas
import re
import datetime

scraper = Scraper()

def get_projects_from_studio(studio_id, studio_num):

    """ Given a list of studio ids, scrapes all projects in those studios into one CSV.

    Args: 
        studios: A list of studio ids to scrape
        studio_num: The number of the studio to output (1-10)
    
    Returns:
        A pandas DataFrame (df) with columns:
            author: the username of the project creator
            project_id: the unique id associated with the scraped project
            studio: the number of the studio (starting from 0) in the list passed to scrape_studios()
    """
    data = []

    print("Scraping studio #" + str(studio_id) + " as studio " + str(studio_num) + ".")
    
    scraped_project_ids = scraper.get_projects_in_studio(studio_id)

    print("Found " + str(len(scraped_project_ids)) + " project ids in studio " + str(studio_num) + ".")

    for project_id in scraped_project_ids:
        new_project_meta = []

        try:
            scraped_meta = scraper.get_project_meta(project_id)
        except Exception:
            scraped_meta = [{'author': {'username': str(Exception)}, 'title': str(Exception), 'instructions': str(Exception), 'description': str(Exception)}]
            print("Unable to scrape project #" + str(project_id) + "due to Exception: " + str(Exception))

        new_project_meta.append(scraped_meta['author']['username'])

        new_project_meta.append(project_id)

        new_project_meta.append(studio_num)

        if scraped_meta["title"] == "": new_project_meta.append("NA") 
        else: new_project_meta.append(" ".join(scraped_meta["title"].split())) # Remove unnecessary whitespace while appending text

        if scraped_meta["instructions"] == "": new_project_meta.append("NA") 
        else: new_project_meta.append(" ".join(scraped_meta["instructions"].split()))

        if scraped_meta["description"] == "": new_project_meta.append("NA") 
        else: new_project_meta.append(" ".join(scraped_meta["description"].split()))

        data.append(new_project_meta)

    print("Finished scraping projects from studio " + str(studio_num))

    # Prepare the columns
    data_columns = ['project_author', 'project_id', 'studio', 'title', 'instructions', 'notes_and_credits']

    # Use pandas to convert our list of lists into a dataframe, with the columns we've specified
    df = pandas.DataFrame(data, columns=data_columns) 

    df.to_csv("scraped_projects\\studio_" + str(studio_num) + ".csv", index=False)

    get_project_comments_from_project_df(df, studio_num)

def get_project_comments_from_project_df(df_projects, studio_num, start_date = datetime.date(1900,1,1), end_date = datetime.date(2100,1,1)):
    """ Given a set of project ids, scrapes all comments posted to those projects.
    
    Args:
        df_projects: a pandas DataFrame (df) of project ids with column header "project_id". Additional columns of "author" and "studio" are helpful. df with all columns can be generated with get_projects_from_list_studios().
        studio_num: The number of the studio to report out (1-10)
        start_date: The date at which we want to start retrieving comments.
        end_date: The date up until which we want to retrieve comments.
    
    Returns:
        A pandas DataFrame of all comments posted in given projects, formatted with the following columns:
            project_author: username of project creator (usually who comment is directed to) 
            commenter: username of commenter (user leaving the comment)
            comment: text of comment
            project_id: unique id of the project on which the comment was left (can be used to find the project on the Scratch website)
            studio: the studio # in which the comment and project were posted
            reply_to: username of the first @ mention in the comment, indicating who the comment replied to
    """

    # Get total number of projects to scrape. Set our number of scraped projects to zero.
    print("Scraping comments from " + str(len(df_projects)) + " projects in " + str(studio_num))
    
    # Create a new, empty list from which to build our final dataframe. Each project and its comments will become a new list within this list, representing a future row in our dataframe
    # e.g. data = [['commenter', 'reply_to', 'comment', 'project_author', 'project_title', 'project_instructions', 'project_notes_and_credits', 'studio', 'project_id', 'timestamp'], [...]]

    data = []

    for row in df_projects.itertuples():
        # Get the project comments using the project id (Returns comments as a list of dictionaries)
        try:
            scraped_comments = scraper.get_project_comments(row.project_id)
        except Exception:
            scraped_comments = [{'username': str(Exception), 'comment': str(Exception), 'timestamp': datetime.datetime(1901,1,1,0,0,0)}] # Report if cannot scrape
            print("Exception on project #" + str(row.project_id) + ": " + str(Exception))
        
        # For each comment (a dictionary) in our list of comments we downloaded,
        for comment in scraped_comments:
            
            comment_timestamp = datetime.datetime.strptime(comment['timestamp'], "%Y-%m-%dT%H:%M:%SZ")

            if start_date <= comment_timestamp.date() <= end_date:

                # Create a new list to append to our dataframe
                new_comment = []

                # Append comment author
                new_comment.append(comment.get('username'))

                # Add a row for whom the comment was addressed to
                match = re.search(r'@.+?\b', str(comment))
                if match:
                    new_comment.append(match.group(0)[1:])
                else:
                    new_comment.append("NA")

                # Substance of comment
                new_comment.append(" ".join(comment["comment"].split())) # Remove unnecessary whitespace while appending comment

                # Project author
                try:
                    new_comment.append(row.project_author)
                except AttributeError:
                    new_comment.append("NA")

                # Project title
                try:   
                    new_comment.append(row.title)
                except AttributeError:
                    new_comment.append("NA")

                # Project instructions
                try:
                    new_comment.append(row.instructions)
                except AttributeError:
                    new_comment.append("NA")

                # Project notes and credits
                try:
                    new_comment.append(row.notes_and_credits)
                except AttributeError:
                    new_comment.append("NA")

                # Studio #
                try:
                    new_comment.append(row.studio)
                except AttributeError:
                    new_comment.append("NA")

                # Project id
                new_comment.append(row.project_id)

                new_comment.append(str(comment_timestamp.date()))

                # Append new comment to our dataframe
                data.append(new_comment)

    # Prepare the columns
    data_columns = ['commenter', 'reply_to', 'comment', 'project_author', 'project_title', 'project_instructions', 'project_notes_and_credits', 'studio', 'project_id', 'timestamp']

    # Use pandas to convert our list of lists into a dataframe, with the columns we've specified
    df = pandas.DataFrame(data, columns=data_columns)
    df.to_csv("scraped_comments\\studio_" + str(studio_num) + ".csv", index = False)
    print("Scraped all comments in studio " + str(studio_num))

if __name__ == "__main__":

    studio_ids = [27021539, 27044045, 27044052, 27044055, 27044057, 27044060, 27044065, 27044071, 27044072, 27044075, 27044081]
    
    jobs = []
    for i in range(11):
        p = multiprocessing.Process(target=get_projects_from_studio, args=(studio_ids[i], i))
        jobs.append(p)
        p.start()

    for proc in jobs:
        proc.join()

    # Combine comments into one file:
    comments_df = pandas.read_csv("scraped_comments//studio_0.csv")
    for i in range(1,11):
        comments_df = pandas.concat([comments_df, pandas.read_csv("scraped_comments//studio_" + str(i) + ".csv")], ignore_index = True)

    comments_df.to_csv("scraped_comments//commentsAllStudios.csv", index = False)

    # Combine projects into one file:
    projects_df = pandas.read_csv("scraped_projects//studio_0.csv")
    for i in range(1,11):
        projects_df = pandas.concat([projects_df, pandas.read_csv("scraped_projects//studio_" + str(i) + ".csv")], ignore_index = True)

    projects_df.to_csv("scraped_projects//allProjects.csv", index = False)