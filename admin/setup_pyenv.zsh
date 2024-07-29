brew update
brew install pyenv
pyenv install 3.10.3
pyenv global 3.10.3

echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
echo '[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"'>> ~/.zshrc
echo 'eval "$(pyenv init -)"'  >> ~/.zshrc

echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zprofile
echo '[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"'>> ~/.zprofile
echo 'eval "$(pyenv init -)"'  >> ~/.zprofile

