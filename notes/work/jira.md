# Jira JQL (canonical)

Если вы используете Jira, сюда можно положить «канонический» JQL для ежедневной выгрузки.

```jql
assignee = currentUser()
AND statusCategory != Done
ORDER BY priority DESC, updated DESC
```

