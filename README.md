# Engineering Shoal School

## Git Deep Dive

We will start with some basic, maybe even boring, but important stuff that may be forgotten or overseen. Then we'll start to go deeper.

- Getting the toe tips wet
    - Standards and Etiquette
        - Commit messages style and best practices
            - Always assume you are NOT alone
            - When applied, this commit will <your commit message> -> imperative mood, future tense
    - Better logs viewing in the CLI
        - Fields and formatting
            - git log --branches --tags --remotes 
            - git log --branches --remotes --tags --date=short --pretty=format:'%C(yellow)%h %C(blue)%ad %C(reset)%>(11,trunc)(%cr) %C(green)%>(17,trunc)%an: %C(white)%s' --graph
- Comfortable at Waiste level water
    - Undoing commits
        - Undo strategies and How and When to use each
    - (Almost) Never use `git pull`
        - Use git `pull --rebase` instead
            - If it works, you're done
            - If not, abort or ...
    - Never fear conflicts
        - How to properly merge conflicts
- Diving deeper
    - Stashing
        - Good uses for git stash
    - Iteractively Rebasing
        - Why, When and How to rebase iteractivelly
        - Edit, undo and squash commits
