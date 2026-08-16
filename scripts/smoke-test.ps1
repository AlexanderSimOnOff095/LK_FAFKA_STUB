$ErrorActionPreference = 'Stop'
$health = Invoke-RestMethod 'http://localhost:8090/api/v1/admin/health'
if ($health.status -ne 'UP') { throw 'STUB health is not UP' }
$topics = Invoke-RestMethod 'http://localhost:8082/topics'
if ($topics -notcontains 'applications.status') { throw 'Kafka topic applications.status is missing' }
$eventId = [guid]::NewGuid().ToString()
$requestId = 'req-' + [guid]::NewGuid().ToString()
$body = @{ records = @(@{ key='smoke-001'; value=@{ eventId=$eventId; eventType='APPLICATION_STATUS_CHANGE_REQUESTED'; eventVersion=1; occurredAt=(Get-Date).ToUniversalTime().ToString('o'); requestId=$requestId; correlationId=$requestId; producer='eapo-cab'; applicationId='smoke-001'; currentStatus='NEW' } }) } | ConvertTo-Json -Depth 8
Invoke-RestMethod 'http://localhost:8082/topics/applications.status' -Method Post -ContentType 'application/vnd.kafka.json.v2+json' -Body $body | Out-Null
Write-Host 'Smoke test request published successfully.'
