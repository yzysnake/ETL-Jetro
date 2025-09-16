# Automation + ETL

## Content

### Auto-Email

### 247 - 24/7 Seafood

### ACME - Acmesomkefish

### Flips - Daily IBTs

### Southern Cross

## How to use

### Install Anaconda is recommended 

```bash
conda create --name basic_work_3135 python=3.13.5
```

### Install packages

```bash
conda actiavte basic_work_3135
cd work
pip install -r requirements.txt
```

### Add Azure Graph API to System Path -- Optional (Auto_email)

```bash
conda activate basic_work_3135
conda env config vars set CLIENT_ID="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" TENANT_ID="yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
conda deactivate
conda activate basic_work_3135
```