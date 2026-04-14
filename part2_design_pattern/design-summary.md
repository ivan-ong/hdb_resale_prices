# HDB Data Platform — Design Summary

## Design Decisions and Trade-offs

### Account Strategy
- **Multi-account isolation** (network, security, data platform, analytics) over a single-account setup — blast radius containment and separation of duties, at the cost of cross-account complexity
- **SCPs restrict to ap-southeast-1** — meets data residency requirements for RESTRICTED classification

### Ingestion Compute: Fargate over Lambda
- Files up to a few GB rule out Lambda (15-min timeout, 10GB ephemeral storage)
- Fargate streams directly to S3 via multipart upload without staging on disk — more resilient and no time limit
- Trade-off: Fargate has slower cold start (~30s vs Lambda's sub-second), acceptable for batch workloads

### Centralized Egress over Per-VPC NAT
- Single inspection point with Network Firewall FQDN allowlist (`*.data.gov.sg`)
- One allowlist to maintain, one set of firewall logs to audit
- Trade-off: adds a network hop through Transit Gateway, but simplifies operations and reduces cost vs duplicating NAT + firewall per VPC

### Firewall Before NAT (Not After)
- Pre-NAT placement preserves original source IPs from spoke VPCs — enables per-workload visibility in firewall logs
- Post-NAT, all traffic appears as one Elastic IP — you lose attribution

### Gateway Endpoint for S3, Interface Endpoints for Everything Else
- S3 Gateway Endpoint: free, no throughput cap, route-table entry — ideal when the caller is in the same VPC
- Interface Endpoints (PrivateLink) for Athena, Glue, STS, KMS: required because these are non-S3 services with no gateway option
- Trade-off: Interface Endpoints cost ~$0.01/AZ/hr + $0.01/GB processed (~$60/month for 4 endpoints across 2 AZs)

### Private DNS on Interface Endpoints
- `athena.ap-southeast-1.amazonaws.com` resolves to the ENI's private IP inside the VPC
- JDBC driver works unchanged — zero configuration on Tableau's side
- Alternative (endpoint-specific hostnames) is fragile and operationally costly

### Three-Zone Data Lake (Raw → Transformed → Curated)
- Raw is immutable, as-received — enables replay and audit
- Transformed converts to Parquet + Snappy — columnar format optimized for Athena scans
- Curated is enriched and partitioned — analysts query only this zone via Lake Formation grants
- Trade-off: storage cost of three copies, justified by clear access boundaries and query performance

### Lake Formation over IAM-Only Access Control
- IAM grants table-level access but cannot mask columns or filter rows
- Lake Formation adds column-level masking and row-level filters — essential for RESTRICTED/PII data
- Cross-account grants let the Analytics Account access curated data without copying it

### Athena Workgroups per Team
- Enforces per-query scan limits (cost guardrail), mandatory SSE-KMS encryption, and pinned result buckets
- Prevents one team's runaway query from affecting others' budgets

---

## Data Flows

### Ingestion Flow
```
EventBridge Scheduler (cron)
  → Step Functions (orchestration, retries, parallel fan-out)
    → ECS Fargate (private subnet, no public IP)
      → Transit Gateway → Network Firewall → NAT Gateway → IGW → data.gov.sg
      → S3 Gateway Endpoint → S3 Raw Bucket (SSE-KMS)
        → Glue Crawler → Glue Data Catalog (schema registration)
        → Glue ETL → S3 Transformed (Parquet/Snappy) → S3 Curated (partitioned)
```

### Exploitation Flow
```
HDB Corporate Network
  → AWS Direct Connect → Transit Gateway → Internal NLB → Tableau Server (EC2, 2 AZs)
    → JDBC → Interface VPC Endpoints (Athena, Glue, STS, KMS)
      → PrivateLink → Athena (reads Glue Catalog + S3 Curated server-side)
      → Query results written to S3 Results Bucket
    → S3 Gateway Endpoint → download results from S3 Results Bucket
```

### Identity Flow
```
HDB User → SAML → IAM Identity Center → IAM Role
  → Lake Formation checks grants (table, column, row-level)
    → Athena enforces permissions at query time
```

---

## Security Posture (Defence in Depth)

| Layer | Controls |
|---|---|
| **Account** | Multi-account isolation, SCPs (region lock) |
| **Network** | No IGW/public IPs in workload VPCs, centralized egress, FQDN allowlist, VPC Flow Logs |
| **Endpoint** | VPC endpoint policies — `aws:PrincipalOrgID` / `aws:ResourceOrgID` (data perimeter) |
| **Identity** | IAM Identity Center, SAML federation, no long-lived credentials |
| **Data** | SSE-KMS (customer-managed keys), bucket policies (`aws:SourceVpce`), Lake Formation (column masking, row filters) |
| **Audit** | CloudTrail org trail, GuardDuty, Security Hub, Config, Macie (PII scanning), Log Archive with Object Lock |
