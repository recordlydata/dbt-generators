FROM mcr.microsoft.com/devcontainers/python:3.11

RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.zsh_history" \
    && echo "$SNIPPET" >> "/root/.zshrc"

RUN apt-get update && apt-get install -y --no-install-recommends \
jq

RUN pip install dbt-snowflake==1.6.2

RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.1.5/zsh-in-docker.sh)" -- \
    -p colored-man-pages \
    -p https://github.com/zsh-users/zsh-autosuggestions

RUN mkdir ~/.oh-my-zsh/completions/
RUN sh -c "curl https://raw.githubusercontent.com/dbt-labs/dbt-completion.bash/master/_dbt > ~/.oh-my-zsh/completions/_dbt"

ENV SHELL /usr/bin/zsh
