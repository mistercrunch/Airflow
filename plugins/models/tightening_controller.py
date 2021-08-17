from sqlalchemy import ForeignKey
from airflow.utils.db import provide_session
from sqlalchemy import Column, String, Integer
from airflow.plugins_manager import AirflowPlugin
from plugins.models.base import Base
from airflow import settings


class TighteningController(Base):
    """
    tightening controllers.
    """

    __tablename__ = "tightening_controller"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    controller_name = Column(String(1000), nullable=False, unique=True)
    line_code = Column(String(1000), nullable=False)
    line_name = Column(String(1000), nullable=True)
    work_center_code = Column(String(1000), nullable=False)
    work_center_name = Column(String(1000), nullable=True)
    device_type_id = Column(Integer, ForeignKey('device_type.id', onupdate='CASCADE', ondelete='SET NULL'),
                            nullable=True)

    def __init__(self, *args, controller_name=None, line_code=None, line_name=None, work_center_code=None,
                 work_center_name=None, device_type_id=None, **kwargs):
        super(TighteningController, self).__init__(*args, **kwargs)
        self.controller_name = controller_name
        self.line_code = line_code
        self.line_name = line_name
        self.work_center_code = work_center_code
        self.work_center_name = work_center_name
        self.device_type_id = device_type_id

    def as_dict(self):
        v: dict = self.__dict__
        if v:
            v.pop('id')
            v.pop('_sa_instance_state')
        return v

    @classmethod
    @provide_session
    def find_controller(cls, controller_name, session=None):
        obj = session.query(cls).filter(cls.controller_name == controller_name).first()
        if obj is None:
            return {}
        return {
            'id': obj.id,
            'controller_name': obj.controller_name,
            'line_code': obj.line_code,
            'line_name': obj.line_name,
            'work_center_code': obj.work_center_code,
            'work_center_name': obj.work_center_name,
            'device_type_id': obj.device_type_id,
        }

    @classmethod
    @provide_session
    def list_controllers(cls, session=None):
        controllers = list(session.query(cls).all())
        return controllers

    @classmethod
    @provide_session
    def add_controller(cls, controller_name, line_code, work_center_code, line_name=None, work_center_name=None,
                       device_type_id=None,
                       session=None):
        session.add(TighteningController(
            controller_name=controller_name,
            line_code=line_code,
            work_center_code=work_center_code,
            line_name=line_name,
            work_center_name=work_center_name,
            device_type_id=device_type_id
        ))

    @staticmethod
    def get_line_code_by_controller_name(controller_name):
        controller_data = TighteningController.find_controller(controller_name)
        if not controller_data:
            raise Exception('未找到控制器数据: {}'.format(controller_name))
        return controller_data.get('line_code', None), controller_data.get('id')


# Defining the plugin class
class TighteningControllerModelPlugin(AirflowPlugin):
    name = "tightening_controller_model_plugin"

    @classmethod
    def on_load(cls):
        engine = settings.engine
        if not engine.dialect.has_table(engine, TighteningController.__tablename__):
            Base.metadata.create_all(engine)
