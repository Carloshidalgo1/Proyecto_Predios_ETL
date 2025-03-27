# Proyecto_predios_ETL
Project proyeccion de predios de la ciudad X vigencias 2024-2025 signature ETL-UAO

# Introduccion
A public agency needs to analyze various variables related to city properties through projections to ensure secure and efficient property tax billing. The goal is to manage the necessary calculations for property analysis and facilitate decision-making by utilizing information extracted from the agency’s licensed software.

As a public city entity, the agency handles large capital figures, making property projections crucial to prevent significant financial losses for both the municipality and taxpayers.
The data extraction phase focuses on creating property tax projections through calculations and analyses comparing previous and current fiscal years (2024-2025).

So, there are numerous difficulties in handling information from the licensed software used by the public agency to project city properties. Poor decision-making and frequent errors occur due to deficiencies in the exported data, which is often extensive and heavy.

# Workflow
A workflow is defined where the data sources are specified as the files used for the established tasks executed via Python/Airflow in Docker.

Transformed data is stored in PostgreSQL databases, following standard ETL phases: Extraction, Staging, Transformation, Merge, and Load. 

![proyecto_predios drawio](https://github.com/user-attachments/assets/66fe6136-5740-483b-8a20-0358a9398a36)

# Dataset Description
The data used for property projection was obtained through the public agency’s licensed software. Due to confidentiality agreements and data sensitivity, certain dataset columns have been modified to prevent compromising the public entity.

The dataset consists of 100,000 records and 21 columns:

●	OBJETO_NUMERICO: Unique indicator assigned by the licensed software to the property owner.

●	TIPOPRED: Property type as defined by regulations.

●	AVALPRED_VIGANT: Previous fiscal year (2024) property valuation.

●	USU_VIGANT: Standardized property indicator for the previous fiscal year.

●	ACTIVIDAD_VIGANT: Standardized property activity for the previous fiscal year.

●	ESTRATO_VIGANT: Socioeconomic stratum for the previous fiscal year.

●	AREA_VIGANT: Property area for the previous fiscal year.

●	TERRENO_VIGANT: Property land value for the previous fiscal year.

●	PREDIAL_VIGANT: Property tax value for the previous fiscal year.

●	COMUNA: Municipality subdivision where the property is located.

●	BARRIO: Neighborhood where the property is located.

●	MANZANA: Block where the property is located.

●	TIPO_PREDIO: Property type used for agency improvements.

●	ACTUALIZACION: Classification of the property as rural or urban.

●	AVALPRED_VIGACT: Current fiscal year (2025) property valuation.

●	USU_VIGACT: Standardized property indicator for the current fiscal year.

●	ACTIVIDAD_VIGACT: Standardized property activity for the current fiscal year.

●	ESTRATO_VIGACT: Socioeconomic stratum for the current fiscal year.

●	AREA_VIGACT: Property area for the current fiscal year.

●	TERRENO_VIGACT: Property land value for the current fiscal year.

●	CARTERA_VIGACT: Indicates whether the property has outstanding debts.

# Tools Used:

●	Python

●	Jupyter Notebooks

●	PostgreSQL

●	Power BI

●	SQLAlchemy

●	Apache Airflow

●	Docker
