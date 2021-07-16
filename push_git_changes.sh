#!/bin/bash

export msg=$1

git config --global user.name 'Koopa Troopa'
git config --global user.email 'koopas@dp6.com.br'

if [[ `git status --porcelain` ]]; then
    git commit -am "${msg}"
    git push
fi

