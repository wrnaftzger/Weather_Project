# Install required packages
install_if_missing <- function(packages) {
  for (pkg in packages) {
    if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
      install.packages(pkg, repos = "https://cloud.r-project.org", quiet = FALSE)
    }
  }
}

packages_needed <- c("DBI", "odbc", "tidyverse", "lubridate", "lme4", "lmerTest")
install_if_missing(packages_needed)

cat("All packages installed successfully\n")