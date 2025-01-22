# Traffic Stops and Race: An Analysis of Police Stops in Fresno

![](https://github.com/catalystcalifornia/fresnoripa/blob/main/FBHC_FullColor_Logo.png)

<li><a href="https://catalystcalifornia.github.io/fresnoripa/report_main">Link to online report [pending]</a></li>

<br>

<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a></li>
    <li>
      <a href="#acknowledgements">Acknowledgements</a></li>
    <li>
      <a href="#built-with">Built With</a></li>
    <li><a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
      </ul>
    </li>
    <li><a href="#data-methodology">Data Methodology</a>
      <ul>
        <li><a href="#data-sources">Data Sources</a></li>
        <li><a href="#data-limitations">Data Limitations</a></li>
      </ul>
    <li><a href="#contact-us">Contact Us</a></li>
    <li><a href="#about-catalyst-california">About Catalyst California</a>
      <ul>
        <li><a href="#our-vision">Our Vision</a></li>
        <li><a href="#our-mission">Our Mission</a></li>
      </ul>
    </li>
    <li><a href="#partners">Partners</a></li>
    <li><a href="#license">License</a></li>
  </ol>
</details>

## About The Project

This report combines data analysis of Fresno Police Department's (“Fresno PD”) patrol activities, quotes and stories from community members in Fresno, and public policy research to show how Fresno PD criminalizes community members through traffic stops and other harmful policing practices. The report unpacks how Fresno PD predominantly conducts traffic stops, and uses traffic stops as a means of profiling and disproportionately harming BIPOC communities. The harms Fresno PD inflicts on BIPOC communities are both physically and emotionally traumatizing, as well as financially damaging in the form of ticketing and citations. Fresno PD's racially biased stop practices undermine community safety and waste public dollars. Their police activities are in opposition to how Fresno residents define safety for their communities, which prioritizes investments in built environment improvements, programming for youth, and mental health and housing services. Information on how community residents define safety and how resources can be reallocated to improve real community safety can be found in the report.

This GitHub repository includes access to our methodology and scripts used to analyze the data and test for racial bias in Fresno PD's policing practices. The repository does not include access to the data tables used for analysis. We pull tables from our private PostgreSQL database. The database is accessible only by our Research & Data Analysis team. The original Racial and Identity Profiling Act (“RIPA”) data used for this project can be accessed via the [California Department of Justice Open Data Portal](https://openjustice.doj.ca.gov/data). For access to the community quotes and stories gathered as a part of this project, please visit the report page.

<p align="right">(<a href="#top">back to top</a>)</p>

## Acknowledgements

Catalyst California completed this research project in collaboration with Fresno Building Healthy Communities (“FBHC”). It was guided by invaluable input from FBHC’s coalition partners and community members. Their perspectives and lived experiences are the driving force behind the data.

The following individuals contributed to the data analysis and visualizations in the report:

* [Elycia Mulholland Graves, Catalyst California](https://github.com/elyciamg)
* [Jennifer Zhang, Catalyst California](https://github.com/jzhang514)
* [Hillary Khan, Catalyst California](https://github.com/hillaryk-ap)
* [Alicia Võ, Catalyst California](https://github.com/avoCC)

A special thank you to Sandra Soria and Isaac Bushnell for their research and data analyst support early in this project.

The following individuals contributed to the framing and writing of the report:

* [Myanna Khalfani-King](https://www.catalystcalifornia.org/who-we-are/staff/myanna-khalfani-king)
* [Michael Nailat](https://www.catalystcalifornia.org/who-we-are/staff/michael-nailat)
* [Chauncee Smith, Catalyst California](https://www.catalystcalifornia.org/who-we-are/staff/chauncee-smith)
* [Elycia Mulholland Graves, Catalyst California](https://github.com/elyciamg)
* [Jennifer Zhang, Catalyst California](https://github.com/jzhang514)

<p align="right">(<a href="#top">back to top</a>)</p>

## Built With

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1b/R_logo.svg/1086px-R_logo.svg.png?20160212050515" alt="R" height="32px"/> &nbsp; <img  src="https://upload.wikimedia.org/wikipedia/commons/d/d0/RStudio_logo_flat.svg" alt="RStudio" height="32px"/> &nbsp; <img  src="https://upload.wikimedia.org/wikipedia/commons/thumb/e/e0/Git-logo.svg/768px-Git-logo.svg.png?20160811101906" alt="RStudio" height="32px"/>

<p align="right">(<a href="#top">back to top</a>)</p>

## Getting Started

To get a local copy up and running follow these simple example steps.

### Prerequisites

We completed the data cleaning, analysis, and visualizations using the following software. 
* [R](https://cran.rstudio.com/)
* [RStudio](https://posit.co/download/rstudio-desktop)

We used several R packages to analyze data and perform different functions, including the following.
* data.table     
* devtools     
* dplyr     
* highcharter     
* htmltools     
* htmlwidgets     
* janitor     
* knitr     
* olsrr     
* purrr     
* readxl     
* RPostgreSQL     
* srvyr     
* stringr     
* tidycensus
* tidyr     
* tidyverse     
* usethis     

Many of our visuals are built with Catalyst California's custom package developed with the highcharter library. The package can be accessed [here](https://github.com/catalystcalifornia/rdaCharts). 

```
list.of.packages <- c("data.table", "devtools", "dplyr", "highcharter", "htmltools", "htmlwidgets", "janitor", "knitr", "olsrr", "purr", "readxl", RPostgreSQL", "srvyr", "stringr", "tidycensus", "tidyr", "tidyverse", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

devtools::install_github("r-lib/usethis")

library(data.table)
library(devtools)
library(dplyr)
library(highcharter)
library(htmltools)
library(htmlwidgets)
library(janitor)
library(knitr)
library(olsrr)
library(purr)
library(readxl)
library(RPostgreSQL)
library(srvyr)
library(stringr)
library(tidycensus)
library(tidyr)
library(tidyverse)
library(usethis)
```

<p align="right">(<a href="#top">back to top</a>)</p>

## Data Methodology

This report evaluates Fresno PD's traffic stop practices by analyzing 2022 data collected by Fresno PD pursuant to the Racial and Identity Profiling Act (“RIPA”) of 2015. RIPA requires law enforcement officers to collect and report information on each stop they conduct, including the time and location, why the stop was conducted and what occurred during it, as well as characteristics about the person stopped (e.g., race, gender, and age). This report examines profiling by Fresno PD by analyzing RIPA data on who officers choose to stop and actions taken by officers during stops. You can access our full methodology [here]( ).

### Data Sources

Police Stop Data	 

* California Department of Justice, “RIPA Stop Data” (reported by Fresno Police Department), 2022,  https://openjustice.doj.ca.gov/data. 

Population Estimates by Race and Sex 

* U.S. Census Bureau, “DP05, B04006, B02018, and B02015,” American Community Survey, 5-Year Estimates, 2018-2022, https://data.census.gov/cedsci/.   
* U.S. Census Bureau, “American Community Survey Public Use Microdata Sample (PUMS)”, 2018-2022, https://www.census.gov/programs-surveys/acs/microdata/access.     

Offense Codes and Statutes

* California Department of Justice, “Law Enforcement Code Tables”, 2023,  https://oag.ca.gov/law/code-tables. 

### Data Limitations

As with all data, our findings depend on the quality of the data collected. We strongly encourage readers and data analysts to consider the limitations of RIPA data when interpreting findings or using RIPA data. For instance, RIPA data are collected under state regulations for all law enforcement agencies and based on officer perception and disclosures. For example, officers report what they perceive as the race(s) of the people they stopped, rather than having the people they stopped self-identify their race(s). Other reports have found evidence of underreporting, misidentification, or even intentional obstruction of information by officers.  Additionally, audits from other jurisdictions have found an undercount in RIPA data, meaning officers report fewer stops in RIPA data compared to the true number of stops that occurred. Lastly, the Fresno RIPA data does not include any geographic information on where the stops occurred, therefore any stop analysis lacks information on which neighborhoods are disproportionately impacted. We encourage researchers using RIPA data to ground truth trends in the data with community to identify discrepancies between the data collected and everyday community experiences. For a full discussion of limitations, please see our [Methodology]( )

<p align="right">(<a href="#top">back to top</a>)</p>


## Contact Us

For policy-related inquiries: 

* [Sandra Celedon](https://es.fresnobhc.org/our-team/sandra-celedon) -  sceledon[at]fresnobhc.org

* [Myanna Khalfani-King](https://www.catalystcalifornia.org/who-we-are/staff/myanna-khalfani-king) - 
mking[at]catalystcalifornia.org

* [Michael Nailat](https://www.catalystcalifornia.org/who-we-are/staff/michael-nailat) - mnailat[at]catalystcalifornia.org

For data-related inquiries: 

* [Elycia Mulholland Graves](https://www.linkedin.com/in/elycia-mulholland-graves-54578258/) - egraves[at]catalystcalifornia.org 

* [Jennifer Zhang](https://www.linkedin.com/in/jenniferzhang3/) - jzhang[at]catalystcalifornia.org

<p align="right">(<a href="#top">back to top</a>)</p>

## About Catalyst California

### Our Vision
A world where systems are designed for justice and support equitable access to resources and opportunities for all Californians to thrive.

### Our Mission
[Catalyst California](https://www.catalystcalifornia.org/) advocates for racial justice by building power and transforming public systems. We partner with communities of color, conduct innovative research, develop policies for actionable change, and shift money and power back into our communities. 

[Click here to view Catalyst California's Projects on GitHub](https://github.com/catalystcalifornia)


<p align="right">(<a href="#top">back to top</a>)</p>

## Partners

### [Fresno Building Healthy Communities](https://es.fresnobhc.org/)

Our Mission: To foster and encourage thriving communities where all children and families can live healthy, safe and productive lives. 


<p align="right">(<a href="#top">back to top</a>)</p>

## Citation

Suggested report citation: Catalyst California and Fresno Building Healthy Communities. “Traffic Stops and Race: An Analysis of Police Stops in Fresno.” 2025.

<p align="right">(<a href="#top">back to top</a>)</p>

## License

Distributed under the General Public Use and Creative Commons Licenses. See LICENSE.txt and CC_LICENSE.md for more information.

<p align="right">(<a href="#top">back to top</a>)</p>
