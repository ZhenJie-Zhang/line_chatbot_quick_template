import os
def test_pic():
    cmd ="""
         ./darknet detector test \
         shoe_person/cfg/obj.data \
         shoe_person/cfg/yolov3.cfg \
         shoe_person/cfg/weights/yolov3_last.weights \
         data/test.jpg \
         -dont_show &
         """
    os.chdir("/root/darknet")
    os.system(cmd)
    print('testing...')
    os.chdir("/root/linechatbot")