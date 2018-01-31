set wildignore+=*/examples/*

set tabstop=2
set shiftwidth=2
set softtabstop=2
set expandtab
set nosmarttab

set textwidth=100

autocmd BufRead *.rs :setlocal tags=./rusty-tags.vi;/
autocmd BufWritePost *.rs :silent! exec "!rusty-tags vi -o --quiet --start-dir=" . expand('%:p:h') . "&" | redraw!

