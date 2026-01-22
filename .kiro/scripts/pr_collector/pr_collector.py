#!/usr/bin/env python3
"""
GitHub PR Review Collector
Collects PR metadata, reviews, and comments for training PR review agents.
"""

import os
import json
import argparse
from datetime import datetime
from typing import List, Dict, Optional
import subprocess


def run_gh_command(cmd: List[str]) -> str:
    """Execute GitHub CLI command and return output."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {' '.join(cmd)}")
        print(f"Error: {e.stderr}")
        raise


def get_pr_list(repo: str, start_date: str, end_date: str, state: str = "all") -> List[Dict]:
    """Get list of PRs in date range."""
    cmd = [
        "gh", "pr", "list",
        "--repo", repo,
        "--state", state,
        "--limit", "1000",
        "--json", "number,title,author,createdAt,closedAt,mergedAt,state,url"
    ]
    
    output = run_gh_command(cmd)
    prs = json.loads(output)
    
    # Filter by date range
    filtered_prs = []
    start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    for pr in prs:
        pr_date = datetime.fromisoformat(pr['createdAt'].replace('Z', '+00:00'))
        if start <= pr_date <= end:
            filtered_prs.append(pr)
    
    return filtered_prs


def get_pr_details(repo: str, pr_number: int) -> Dict:
    """Get detailed PR information including reviews and comments."""
    # Get PR details
    cmd = [
        "gh", "pr", "view", str(pr_number),
        "--repo", repo,
        "--json", "number,title,body,author,createdAt,closedAt,mergedAt,state,url,additions,deletions,changedFiles,labels,assignees,reviewRequests"
    ]
    pr_data = json.loads(run_gh_command(cmd))
    
    # Get reviews
    cmd = [
        "gh", "api",
        f"/repos/{repo}/pulls/{pr_number}/reviews",
        "--jq", "."
    ]
    reviews = json.loads(run_gh_command(cmd))
    
    # Get review comments (inline code comments)
    cmd = [
        "gh", "api",
        f"/repos/{repo}/pulls/{pr_number}/comments",
        "--jq", "."
    ]
    review_comments = json.loads(run_gh_command(cmd))
    
    # Get issue comments (general PR comments)
    cmd = [
        "gh", "api",
        f"/repos/{repo}/issues/{pr_number}/comments",
        "--jq", "."
    ]
    issue_comments = json.loads(run_gh_command(cmd))
    
    # Get files changed
    cmd = [
        "gh", "api",
        f"/repos/{repo}/pulls/{pr_number}/files",
        "--jq", "."
    ]
    files = json.loads(run_gh_command(cmd))
    
    pr_data['reviews'] = reviews
    pr_data['review_comments'] = review_comments
    pr_data['issue_comments'] = issue_comments
    pr_data['files'] = files
    
    return pr_data


def format_pr_markdown(pr_data: Dict) -> str:
    """Format PR data as markdown."""
    md = []
    
    # Header
    md.append(f"# PR #{pr_data['number']}: {pr_data['title']}\n")
    md.append(f"**URL:** {pr_data['url']}\n")
    md.append(f"**Author:** @{pr_data['author']['login']}\n")
    md.append(f"**Created:** {pr_data['createdAt']}\n")
    md.append(f"**State:** {pr_data['state']}\n")
    
    if pr_data.get('mergedAt'):
        md.append(f"**Merged:** {pr_data['mergedAt']}\n")
    elif pr_data.get('closedAt'):
        md.append(f"**Closed:** {pr_data['closedAt']}\n")
    
    md.append(f"**Changes:** +{pr_data.get('additions', 0)} -{pr_data.get('deletions', 0)} ({pr_data.get('changedFiles', 0)} files)\n")
    
    # Labels
    if pr_data.get('labels'):
        labels = [f"`{label['name']}`" for label in pr_data['labels']]
        md.append(f"**Labels:** {', '.join(labels)}\n")
    
    # Assignees
    if pr_data.get('assignees'):
        assignees = [f"@{a['login']}" for a in pr_data['assignees']]
        md.append(f"**Assignees:** {', '.join(assignees)}\n")
    
    # Description - filter out checklist sections
    md.append("\n## Description\n")
    body = pr_data.get('body', '_No description provided_')
    if body and body != '_No description provided_':
        # Remove checklist sections (lines starting with - [ ] or - [x])
        lines = body.split('\n')
        filtered_lines = []
        in_checklist = False
        for line in lines:
            stripped = line.strip()
            # Detect checklist items
            if stripped.startswith('- [ ]') or stripped.startswith('- [x]') or stripped.startswith('- [X]'):
                in_checklist = True
                continue
            # Skip empty lines immediately after checklist
            if in_checklist and not stripped:
                in_checklist = False
                continue
            in_checklist = False
            filtered_lines.append(line)
        body = '\n'.join(filtered_lines).strip()
    md.append(body)
    md.append("\n")
    
    # Skip Files Changed section
    
    # Reviews - filter out CodeRabbit and include comment content
    md.append("\n## Reviews\n")
    has_reviews = False
    for review in pr_data.get('reviews', []):
        reviewer = review['user']['login']
        
        # Skip CodeRabbit reviews
        if 'coderabbit' in reviewer.lower() or 'bot' in reviewer.lower():
            continue
        
        state = review['state']
        
        # Only include reviews with actual comments
        if review.get('body') and review['body'].strip():
            has_reviews = True
            md.append(f"\n### @{reviewer} - {state}\n")
            md.append(f"\n{review['body']}\n")
    
    if not has_reviews:
        md.append("\n_No human reviews with comments_\n")
    
    # Review Comments (inline code comments) - filter out CodeRabbit
    md.append("\n## Review Comments\n")
    has_review_comments = False
    for comment in pr_data.get('review_comments', []):
        author = comment['user']['login']
        
        # Skip CodeRabbit comments
        if 'coderabbit' in author.lower() or 'bot' in author.lower():
            continue
        
        path = comment['path']
        line = comment.get('line', comment.get('original_line', 'N/A'))
        
        has_review_comments = True
        md.append(f"\n### @{author} on `{path}:{line}`\n")
        md.append(f"\n{comment['body']}\n")
    
    if not has_review_comments:
        md.append("\n_No inline code comments from humans_\n")
    
    # Issue Comments (general PR comments) - filter out CodeRabbit
    md.append("\n## General Comments\n")
    has_general_comments = False
    for comment in pr_data.get('issue_comments', []):
        author = comment['user']['login']
        
        # Skip CodeRabbit comments
        if 'coderabbit' in author.lower() or 'bot' in author.lower():
            continue
        
        has_general_comments = True
        md.append(f"\n### @{author}\n")
        md.append(f"\n{comment['body']}\n")
    
    if not has_general_comments:
        md.append("\n_No general comments from humans_\n")
    
    return '\n'.join(md)


def save_pr_data(repo: str, start_date: str, end_date: str, pr_data_list: List[Dict]):
    """Save PR data to markdown file."""
    # Create directory structure
    repo_name = repo.split('/')[-1]
    date_range = f"{start_date[:10]}-TO-{end_date[:10]}"
    
    # Try workspace .kiro first, fall back to home directory
    workspace_dir = os.path.join(os.getcwd(), ".kiro", "resources", repo_name)
    home_dir = os.path.expanduser(f"~/.kiro/resources/{repo_name}")
    
    # Prefer workspace if .kiro exists in current directory
    if os.path.exists(os.path.join(os.getcwd(), ".kiro")):
        output_dir = workspace_dir
    else:
        output_dir = home_dir
    
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f"{date_range}.md")
    
    with open(output_file, 'w') as f:
        f.write(f"# PR Review Data: {repo}\n")
        f.write(f"**Date Range:** {start_date[:10]} to {end_date[:10]}\n")
        f.write(f"**Total PRs:** {len(pr_data_list)}\n")
        f.write(f"**Generated:** {datetime.now().isoformat()}\n")
        f.write("\n---\n\n")
        
        for pr_data in pr_data_list:
            f.write(format_pr_markdown(pr_data))
            f.write("\n\n---\n\n")
    
    print(f"✓ Saved PR data to: {output_file}")
    return output_file


def main():
    parser = argparse.ArgumentParser(description="Collect GitHub PR review data")
    parser.add_argument("--repo", required=True, help="Repository (owner/repo)")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--state", default="all", choices=["open", "closed", "merged", "all"], help="PR state filter")
    parser.add_argument("--limit", type=int, help="Limit number of PRs to collect")
    
    args = parser.parse_args()
    
    # Convert dates to ISO format
    start_date = f"{args.start_date}T00:00:00Z"
    end_date = f"{args.end_date}T23:59:59Z"
    
    print(f"Collecting PRs from {args.repo}...")
    print(f"Date range: {args.start_date} to {args.end_date}")
    
    # Get PR list
    prs = get_pr_list(args.repo, start_date, end_date, args.state)
    print(f"Found {len(prs)} PRs in date range")
    
    if args.limit:
        prs = prs[:args.limit]
        print(f"Limited to {len(prs)} PRs")
    
    # Collect detailed data for each PR
    pr_data_list = []
    for i, pr in enumerate(prs, 1):
        print(f"Collecting PR #{pr['number']} ({i}/{len(prs)})...")
        try:
            pr_details = get_pr_details(args.repo, pr['number'])
            pr_data_list.append(pr_details)
        except Exception as e:
            print(f"Error collecting PR #{pr['number']}: {e}")
            continue
    
    # Save to file
    output_file = save_pr_data(args.repo, start_date, end_date, pr_data_list)
    print(f"\n✓ Collection complete!")
    print(f"✓ Collected {len(pr_data_list)} PRs")
    print(f"✓ Output: {output_file}")


if __name__ == "__main__":
    main()
