import os
from tqdm import tqdm
import shutil

def move():
    # /home/balajic/workdir/EXPERIMENTS/donut/apollo/data/interim_df_train/18123397-Amzn:Root=1-656948c4-1685d1ae758967017ffa7467-page_1.jpeg.csv
    claims_path = '/home/balajic/workdir/donut_training_pipeline/images/extraction_finished/'
    interim_data_path = '/home/balajic/workdir/donut_training_pipeline/interim_df/extraction_finished/'
    dest_train_path = '/home/balajic/workdir/donut_training_pipeline/dataset/train/'
    dest_val_path = '/home/balajic/workdir/donut_training_pipeline/dataset/validation/'
    # dest_test_path = '/home/balajic/workdir/EXPERIMENTS/donut/one_pagers/dataset/test/'

    interim_csv_files = os.listdir(interim_data_path)
    image_files = [i.replace('.csv', '') for i in interim_csv_files]
    total_images = len(image_files)
    print("Total Extraction Finished Images are: ", total_images)

    for img_count, image in tqdm(enumerate(image_files)):

        claim = image.split('-')[0]

        temp_imgname = image.replace(f'{claim}-', '')
        if img_count <= (0.80 * total_images) :
            shutil.copyfile(claims_path + claim + '/' + temp_imgname, dest_train_path + image)
        # elif img_count <= (0.85 * total_images):
        #     shutil.copyfile(dec_claims_path + claim + '/' + temp_imgname, dest_val_path + image)
        else:
            shutil.copyfile(claims_path + claim + '/' + temp_imgname, dest_val_path + image)

    print("Successfully copied all images")

    return None

if __name__ == '__main__':
    move()