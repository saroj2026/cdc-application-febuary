#!/bin/bash
# Script to merge commits into Development branch

echo "=== Merging commits to Development branch ==="

# Check current branch and status
echo "Current branch:"
git branch --show-current

echo ""
echo "Recent commits:"
git log --oneline -5

echo ""
echo "Checking for uncommitted changes..."
git status --short

# Get current branch name
CURRENT_BRANCH=$(git branch --show-current)
echo ""
echo "Current branch: $CURRENT_BRANCH"

# Check if Development branch exists
if git show-ref --verify --quiet refs/heads/Development; then
    echo "Development branch exists locally"
    git checkout Development
else
    if git show-ref --verify --quiet refs/remotes/origin/Development; then
        echo "Development branch exists on remote, creating local branch..."
        git checkout -b Development origin/Development
    else
        echo "Creating new Development branch..."
        git checkout -b Development
    fi
fi

# Merge the commits from the previous branch
echo ""
echo "Merging $CURRENT_BRANCH into Development..."
git merge $CURRENT_BRANCH --no-edit

if [ $? -eq 0 ]; then
    echo ""
    echo "Merge successful!"
    echo ""
    echo "Pushing to origin/Development..."
    git push origin Development
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "=== SUCCESS: All commits merged and pushed to Development branch! ==="
        echo ""
        echo "Final status:"
        git log --oneline -5
    else
        echo ""
        echo "ERROR: Failed to push. You may need to set upstream:"
        echo "  git push -u origin Development"
    fi
else
    echo ""
    echo "WARNING: Merge had conflicts. Please resolve them manually."
    echo "After resolving conflicts, run:"
    echo "  git add ."
    echo "  git commit"
    echo "  git push origin Development"
fi

