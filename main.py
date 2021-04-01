import sys
import argparse
import configparser
import logging
import os.path
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.storage.blob import ResourceTypes, AccountSasPermissions
from azure.storage.blob import generate_account_sas

def create_token_sas():
    """
    Cette fonction crée un token d'une durée de vie d'une heure
    elle renvoie et print le token
    """
    sas_token = generate_account_sas(
        account_name=(f"{config['storage']['account']}"),
        account_key=(f"{config['storage']['key']}"),
        resource_types=ResourceTypes(object=True),
        permission=AccountSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )
    print(sas_token)
    return sas_token

def listb(containerclient):
    """
    cette fonction retourne la liste des blobs contenu un container
    elle prend en paramètre un container
    """
    blob_list=containerclient.list_blobs()
    for blob in blob_list:
        print(blob.name)


def upload(cible, blobclient):
    """
    cette fonction permet d'uploader un fichier sous forme de blob dans un container
    elle prend en paramètres le chemin du fichier en string
    le container cible
    """
    with open(cible, "rb") as f:
        blobclient.upload_blob(f)


def download(filename, dl_folder, blobclient):
    """
    cette fonction permet de télécharger un fichier à partir d'un blob
    elle prend en paramètre:
    le nom du blob a télécharger
    le dossier de desination du téléchargement
    le container qui contient le blob
    """
    with open(os.path.join(dl_folder,filename), "wb") as my_blob:
        blob_data=blobclient.download_blob()
        blob_data.readinto(my_blob)


def main(args,config):
    """
    fonction centrale du projet qui permet d'appeler les autres fonctions et leur paramètre avec le parser
    """
    blobclient=BlobServiceClient(
        f"https://{config['storage']['account']}.blob.core.windows.net",
        config["storage"]["key"],
        logging_enable=False)
    containerclient=blobclient.get_container_client(config["storage"]["container"])
    if args.action=="list":
        return listb(containerclient)
    else:
        if args.action=="upload":
            blobclient=containerclient.get_blob_client(os.path.basename(args.cible))
            return upload(args.cible, blobclient)
        elif args.action=="download":
            blobclient=containerclient.get_blob_client(os.path.basename(args.remote))
            return download(args.remote, config["general"]["restoredir"], blobclient)
        elif args.action=="token":
            create_token_sas()
    

if __name__=="__main__":
    parser=argparse.ArgumentParser("Logiciel d'archivage de documents")
    parser.add_argument("-cfg",default="config.ini",help="chemin du fichier de configuration")
    parser.add_argument("-lvl",default="info",help="niveau de log")
    subparsers=parser.add_subparsers(dest="action",help="type d'operation")
    subparsers.required=True
    
    parser_s=subparsers.add_parser("upload")
    parser_s.add_argument("cible",help="fichier à envoyer")

    parser_r=subparsers.add_parser("download")
    parser_r.add_argument("remote",help="nom du fichier à télécharger")
    parser_r=subparsers.add_parser("list")
    parser_s=subparsers.add_parser("token")
    args=parser.parse_args()

    #erreur dans logging.warning : on a la fonction au lieu de l'entier
    loglevels={
        "debug":logging.DEBUG,
        "info":logging.INFO,
        "warning":logging.WARNING,
        "error":logging.ERROR,
        "critical":logging.CRITICAL
    }
    print(loglevels[args.lvl.lower()])
    logging.basicConfig(level=loglevels[args.lvl.lower()])

    config=configparser.ConfigParser()
    config.read(args.cfg)

    sys.exit(main(args,config))