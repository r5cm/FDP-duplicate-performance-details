# Get data --------------------------------------------------------------------------

# Login to Salesforce
library(RForcecom)
password <- "lestari45PtkwZcuVt5G3DtiK39kq30MnI"
session <- rforcecom.login("reports@utzmars.org", password)

# Retrieve performance details
perf.det.md <- rforcecom.getObjectDescription(session, "Performance_detail__c")
user.perf.md <- rforcecom.getObjectDescription(session, "User_performance__c")
fields <- c("Id", "Agree__c", "FDP_Submission__c", "createdDate", "Farmer_name__c",
            "Farmer_code__c", "User_performance__c", "User_performance__r.Activity__c",
            "User_performance__r.Performance__c", "User_performance__r.Mars_profile__c",
            "User_performance__r.Assigned_to_Name__c")
perf.det <- rforcecom.retrieve(session, "Performance_detail__c", fields)
names(perf.det) <- c("Id", "agree", "submission", "createdDate", "farmerName", 
                     "farmerCode", "user.performance", "up.activity", "up.performance",
                     "up.profile", "user")

# Exclude monitoring activities AND performance==NA AND profile contains FC
perf.det <- perf.det[perf.det$up.activity == "FDP diagnostic" & 
                           !is.na(perf.det$up.performance) &
                           grepl("Field Coordinator", perf.det$up.profile) &
                           perf.det$agree == "Ya", ]

# Identify repeated ---------------------------------------------------------------

# Identify repeated perf.det (relationship with the same submission)
up.freq <- as.data.frame(table(perf.det$submission))
up.freq.rep <- up.freq[up.freq$Freq > 1, ]

# List of repeated to delete (leave only first)
library(dplyr)
perf.det.dup <- perf.det[perf.det$submission %in% up.freq.rep$Var1, ]
perf.det.dup$createdDate <- as.character(substr(perf.det.dup$createdDate, 1, 10))
perf.det.dup <- perf.det.dup[order(perf.det.dup$createdDate), ]
perf.det.rm <- perf.det.dup[duplicated(perf.det.dup$submission), ]
# Export list of duplicates (DO NOT OVERWRITE EXISTING FILES!!!!)
dup.export <- select(perf.det.dup, user, farmerName, farmerCode, createdDate)
dup.export <- dup.export[order(dup.export$user, dup.export$farmerName), ]
library(xlsx)
today.date <- Sys.Date()
write.xlsx(dup.export, 
           paste("User with duplicate FDP agreements ", today.date,".xlsx", sep = ""),
           row.names = FALSE)


# Delete repeated -------------------------------------------------------------------

# Login as admin
password.adm <- "gfutzmars2018*hn5OC4tzSecOhgHKnUtZL05C"
session.admin <- rforcecom.login("admin@utzmars.org", password.adm)

# run a delete job into the Account object
job_info <- rforcecom.createBulkJob(session.admin, 
                                    operation='delete', 
                                    object='Performance_detail__c')
# split into batch sizes of 500 (2 batches for our 1000 row sample dataset)
batches_info <- rforcecom.createBulkBatch(session.admin, 
                                          jobId=job_info$id, 
                                          select(perf.det.rm, Id), 
                                          multiBatch = TRUE, 
                                          batchSize=500)
# check on status of each batch
batches_status <- lapply(batches_info, 
                         FUN=function(x){
                               rforcecom.checkBatchStatus(session.admin, 
                                                          jobId=x$jobId, 
                                                          batchId=x$id)
                         })
# get details on each batch
batches_detail <- lapply(batches_info, 
                         FUN=function(x){
                               rforcecom.getBatchDetails(session.admin, 
                                                         jobId=x$jobId, 
                                                         batchId=x$id)
                         })
# close the job
close_job_info <- rforcecom.closeBulkJob(session.admin, jobId=job_info$id)

# Compare to existing FDP submissions agreed
comp.pd <- select(perf.det, Id, submission)
query.sub <- "SELECT Id FROM FDP_submission__c WHERE agreedRecommendations__c = 'Ya'"
comp.sub <- rforcecom.query(session, query.sub)
# Performance details contained in submissions
table(comp.pd$submission %in% comp.sub$Id)
# Submissions contained in performance details
table(comp.sub$Id %in% comp.pd$submission)
# Identify submission that is missing from 
comp.sub[!(comp.sub$Id %in% comp.pd$submission), ]
