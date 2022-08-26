import glob
import json
import os
from typing import List

import psycopg2


table_insert_actor = """
    INSERT INTO actor (
        actor_id,login,display_login,gravatar_id,url,avatar_url
    ) VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (actor_id) DO NOTHING
"""
table_insert_repos = """
    INSERT INTO repo (
        repo_id,name,url
    ) VALUES (%s,%s,%s)
    ON CONFLICT (repo_id) DO NOTHING
"""
table_insert_orgs = """
    INSERT INTO org (
        orgs_id,login,gravatar_id,url,avatar_url
    ) VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (orgs_id) DO NOTHING
"""
table_insert_events = """
    INSERT INTO events VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (events_id) DO NOTHING
"""



def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                #print(each["id"], each["type"], each["actor"]["login"])

                # Insert data into tables_actor
                actor_list=[]
                res_actor = []
                for actor in each["actor"]:
                    actor_list.append(actor)
                [res_actor.append(x) for x in actor_list if x not in res_actor]
                if each['actor'][res_actor[3]] !='':
                    col_actor= each['actor'][res_actor[0]],each['actor'][res_actor[1]],each['actor'][res_actor[2]],each['actor'][res_actor[3]],repr(each['actor'][res_actor[4]]),repr(each['actor'][res_actor[5]])
                    cur.execute(table_insert_actor,col_actor)
                    conn.commit()
                else:
                    col_actor= each['actor'][res_actor[0]],each['actor'][res_actor[1]],each['actor'][res_actor[2]],"NULL",repr(each['actor'][res_actor[4]]),repr(each['actor'][res_actor[5]])
                    cur.execute(table_insert_actor,col_actor)
                    conn.commit()
                
                # Insert data into tables_repos
                repo_list=[]
                res_repo = []
                for repo in each["repo"]:
                    repo_list.append(repo)
                [res_repo.append(x) for x in repo_list if x not in res_repo]
                col_repo= each['repo'][res_repo[0]],each['repo'][res_repo[1]],each['repo'][res_repo[2]]
                cur.execute(table_insert_repos,col_repo)
                conn.commit()

                # Insert data into tables_orgs
                orgs_list=[]
                res_orgs = []
                try:
                    for org in each["org"]:
                        orgs_list.append(org)
                except:
                    pass
                [res_orgs.append(x) for x in orgs_list if x not in res_orgs]
                try:
                    if each['org'][res_orgs[2]] !='':
                        col_orgs= each['org'][res_orgs[0]],each['org'][res_orgs[1]],each['org'][res_orgs[2]],repr(each['org'][res_orgs[3]]),repr(each['org'][res_orgs[4]])
                        cur.execute(table_insert_orgs,col_orgs)
                        conn.commit()
                    else:
                        col_orgs= each['org'][res_orgs[0]],each['org'][res_orgs[1]],"NULL",repr(each['org'][res_orgs[3]]),repr(each['org'][res_orgs[4]])
                        cur.execute(table_insert_orgs,col_orgs)
                        conn.commit()
                except:
                    pass
                
                # Insert data into tables_events
                try:
                    col_events = each["id"],each['actor']["id"],each['repo']["id"],each["type"],each["public"],each["created_at"],each['org']["id"]
                    cur.execute(table_insert_events,col_events)
                    conn.commit()
                except:
                    table_insert_events2 = """
                        INSERT INTO events (events_id,actor_id,repo_id,type,public,created_at) VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (events_id) DO NOTHING
                        """
                    col_events = each["id"],each['actor']["id"],each['repo']["id"],each["type"],each["public"],each["created_at"],
                    cur.execute(table_insert_events2,col_events)
                    conn.commit()
                
                    
#col_events = each["id"],each['actor'][res_actor[0]],each['repo'][res_repo[0]],each["type"],each["public"],each["created_at"],each['org'][res_orgs[0]]

def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../Data")

    conn.close()


if __name__ == "__main__":
    main()