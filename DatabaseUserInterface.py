import psycopg2
import csv
import simplejson
import scipy
from decimal import Decimal, getcontext     #for fishers_method



#***main functions--------------------------------------------------------------------------------------------------***


def openConnection() -> type(tuple()):
    try:
        # Establish a connection to the database
        db_config = {
        'database': 'sdp152024',
        'host': 'seniordesign.cyzdvv3sqno4.us-east-1.rds.amazonaws.com',
        'port': '5432',
        'user': 'postgres',
        'password': 'Uconn!2024'
        }

        #Connect to the database with config
        connectionInstance = psycopg2.connect(**db_config)

        # Create a cursor object to interact with the database
        cursor = connectionInstance.cursor()

        # print("Connected to the database!")
        if connectionInstance.status == psycopg2.extensions.STATUS_READY:
            return connectionInstance, cursor

    except Exception as e:
        print(f"Error: Unable to connect to the database - {e}")
        raise


#         gene is type str     return type is tuple data(list) and str (json str)
def searchByGene(gene: str) -> tuple[list,str]:
    connection, cursor = openConnection()

    query = """SELECT * FROM \"GENE\" WHERE \"GeneID\" = %s ORDER BY \"p_Value\" ASC;"""

    cursor.execute(query, (gene,))
        
    output = cursor.fetchall() 
    
    connection.close()
    return list(output), simplejson.dumps(output, indent=2)
#   **returns tuple access the elements 0 and 1 accordingly**


def searchByMesh(mesh: str) -> tuple[list,str]:
    connection, cursor = openConnection()

    query = """SELECT * FROM \"GENE\" WHERE \"MeSH\" LIKE %s ORDER BY \"p_Value\" ASC;"""

    cursor.execute(query, (mesh,))
    
    output = cursor.fetchall() 

    connection.close()
    return list(output), simplejson.dumps(output, indent=2)


def multipleByGene(geneList: list) ->  tuple[list,str]:
    connection, cursor = openConnection()

    query="""SELECT DISTINCT ARRAY_AGG("p_Value")AS pVals,A."MeSH",COUNT(DISTINCT "GeneID")AS numGenes,ARRAY_AGG("GeneID" ORDER BY "GeneID")AS listGenes
    FROM "GENE"AS A
    WHERE A."MeSH"IN(SELECT B."MeSH"FROM "GENE"AS B WHERE B."GeneID"=%s)
    GROUP BY A."MeSH"
    ORDER BY 4;"""

    output = []
    for gene in geneList:
        cursor.execute(query, (gene,))  #format for query,data
        rows = cursor.fetchall()

        for row in rows:
            #type is tuple cast to list
            temp=list(row)

            #[0] is the index of a list of pvalues
            if len(temp[0])>1:
                #fishers_method() returns in string form for representation
                temp[0]=fishers_method(temp[0])
            else:
                #otherwise the first element is a list with one element, get that one element
                temp[0]=temp[0][0]

            output.append(temp)

    connection.close()
    return output, simplejson.dumps(output, indent=2)


def listAllGene() -> list[str]:
    connection, cursor = openConnection()

    cursor.execute(
        f"SELECT DISTINCT \"GeneID\" FROM \"GENE\" ORDER BY \"GeneID\" ASC;")
    
    data = cursor.fetchall()

    connection.close()
    return [str(i[0]) for i in data]


def listAllMesh() -> list[str]:
    connection, cursor = openConnection()

    cursor.execute(
        f"SELECT \"MeSH\" FROM (SELECT * FROM \"GENE\" ORDER BY \"p_Value\" ASC) AS A;")
    
    data = cursor.fetchall()
    
    connection.close()
    return [str(i[0]) for i in data]



#***helper functions------------------------------------------------------------------------------------------------***


def fishers_method(p_values: list) -> str:
    #Precision set based on smallest value i.e. ~1.0e-319
    getcontext().prec = 319

    #cast trickery frontloads decimal places otherwise it would approximate to 0
    p_values_decimal = [Decimal(p) for p in p_values]
    p_values_float = [float(p) for p in p_values_decimal]
    try:
        combined_result = scipy.stats.combine_pvalues(p_values_float, method="fisher")

        #returns as string -- can be changed with 
        combined_p_value = str(float(combined_result[1]))
        return combined_p_value
    except Exception as e:
        return str(0.0) #change this cast if changing to float



#***testing functions-----------------------------------------------------------------------------------------------***


def writeJsonToTxt(data: list, fileName: str = 'Demo.txt') -> None:
    with open(f"{fileName}","w") as f:
        f.writelines(data)

    return None


def writeToCsvFile(data: list, fileName: str = 'Demo.csv') -> None:
    with open(f"{fileName}","w") as f:
        csv_out = csv.writer(f)
        csv_out.writerow(["GeneID", "MeSH", "p_value", "Enrich", "PMIDs"])
        csv_out.writerows(data)

    return None


def main() -> None:

    output = multipleByGene(geneList=[34,37])

    # output = searchByMesh(mesh='Acyl-CoA Dehydrogenase')  #* manual use *
    # output = searchByGene(gene=34)                        #* manual use *


    # print(output[0])
    writeToCsvFile(data=output[0])
    writeJsonToTxt(data=output[1])

    print("\nDone!\n")
    return
#}


if __name__ == "__main__":
    main()
