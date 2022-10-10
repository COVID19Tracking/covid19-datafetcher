#!/bin/sh

cd covid19-datafetcher

# update repo
GIT_HEAD=$(git rev-parse HEAD)
# git checkout master?
git fetch; git rebase origin/master
NEW_COMMITS=$(git rev-list --pretty $GIT_HEAD..HEAD)

if [ -n "$NEW_COMMITS" ]; then
    # Notify about new commits
    echo $(date)
    echo $NEW_COMMITS
fi


# run fetch
conda run -n c19-data python get_my_data.py dataset=states
conda run -n c19-data python tools/push_to_spreadsheet.py push.spreadsheet_id=1brHKBhqiXkkLyiDTDfaBK-tms-R4KVkI-NPFfFZwqYk push.sheet_id=0 push.file=outputs/states.csv creds.type=service creds.key_filepath=../creds/credentials.json
conda run -n c19-data python tools/push_to_spreadsheet.py push.spreadsheet_id=1brHKBhqiXkkLyiDTDfaBK-tms-R4KVkI-NPFfFZwqYk push.sheet_id=289169575 push.file=outputs/covid_vaccination_US.csv creds.type=service creds.key_filepath=../creds/credentials.json
