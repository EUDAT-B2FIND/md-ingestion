alias ll='ls -l'
alias llt='ls -lgort'
alias llh='ls -lhrtgG'  #in kB,without users, time sorted
alias h='history'
#alias ..='cd ..'
#alias ...='cd ../..'
#alias ....='cd ../../..'

# store history in project folder
export PROMPT_COMMAND='history -a'
export HISTFILE="/workspaces/md-ingestion/.bash_history"

# enable bash completion
. /etc/bash_completion
