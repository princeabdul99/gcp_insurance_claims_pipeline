# Insurance Claims Decision Management System

A decision-centric data engineering system that operationalizes insurance claims data into measurable, governed business decisions with clear financial impact.

Problem
========

Insurance claims operations suffer from:

- Slow manual processing
- Inaccurate reserves
- Capital lock up
- Loss leakage


Solution
================

A **Decision Management System** that converts claims, policy, reserve, and payment data into **explicit, auditable operational decisions**.

### Decisions Enabled
- Claim handling (auto-approve / review / escalate)
- Reserve adequacy management
- Payment authorization

### How Decision Are Measured
- Automation rate
- Claim cycle time
- Reserve accuracy
- Loss leakage

### Valued Delivered
- Reduced manual processing cost
- Optimized reserve capital
- Improved financial control over claims payouts


Technical Implementation
=========================
![image_alt](https://github.com/princeabdul99/gcp_insurance_claims_pipeline/blob/1810a7608e62655720ef17aeda2ece1b23a2523b/docs/adlc.png)


### Architecture
![image_alt]()
The architecture follows a layered design:

- **Bronze**: Raw claim, policy, payment, and reserve events
- **Silver**: Cleaned, conformed insurance entities and facts
- **Gold**: Decision features, decision outputs, and value metrics
