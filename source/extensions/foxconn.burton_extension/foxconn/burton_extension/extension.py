import json
import math
import omni.ext
import omni.ui as ui
import omni.usd
import omni.kit.app
from pxr import Usd, UsdGeom, Gf
from kafka import KafkaConsumer

BROKER = "localhost:9092"

KAFKA_TOPIC = "robot_states"

ROBOT_MAP = {
    "W1000000001": "/World/fiibot_w1_v2_260320",
    "W1000000002": "/World/fiibot_w1_v2_260321",
}

INVERT = {
    "left_3",
    "right_3",
    "head_pitch",
}

GRIP_AXIS = 0
GRIP_MIN = 0.0
GRIP_MAX = 0.05

REF_POINTS = {
    "W1000000001": {"x": 129.4, "y": 41.0, "yaw": 1.57},
    "W1000000002": {"x": 125.0, "y": 41.0, "yaw": 1.57},
}


class MapAligner:
    def __init__(self):
        self.offset_x = 0.0
        self.offset_y = 0.0
        self.alpha = 0.0

    def calibrate(self, map_x, map_y, map_theta, scene_x, scene_y, scene_yaw):
        self.alpha = scene_yaw - map_theta
        rx = map_x * math.cos(self.alpha) - map_y * math.sin(self.alpha)
        ry = map_x * math.sin(self.alpha) + map_y * math.cos(self.alpha)
        self.offset_x = scene_x - rx
        self.offset_y = scene_y - ry

    def to_scene(self, map_x, map_y, map_theta):
        X = map_x * math.cos(self.alpha) - map_y * math.sin(self.alpha) + self.offset_x
        Y = map_x * math.sin(self.alpha) + map_y * math.cos(self.alpha) + self.offset_y
        yaw = map_theta + self.alpha
        return X, Y, yaw

    def reset(self):
        self.offset_x = 0.0
        self.offset_y = 0.0
        self.alpha = 0.0

    def describe(self):
        return (f"alpha={math.degrees(self.alpha):.1f}deg "
                f"offset=({self.offset_x:.3f}, {self.offset_y:.3f})")


class RobotDriver:

    def __init__(self, stage, robot_id, root_path):
        self.robot_id = robot_id
        self.root = root_path
        self.ref = REF_POINTS.get(robot_id, {"x": 0.0, "y": 0.0, "yaw": 0.0})

        self._rot_ops = {}
        self._grip_ops = {}
        self._base_translate = None
        self._base_rotate = None
        self._jack_op = None
        self._aligner = MapAligner()
        self._earliest_pose = None

        self._cache_nodes(stage)

    def _cache_nodes(self, stage):
        root_prim = stage.GetPrimAtPath(self.root)
        if not root_prim or not root_prim.IsValid():
            print(f"[fiibot {self.robot_id}] root NOT found: {self.root}")
            return

        for prim in Usd.PrimRange(root_prim):
            name = prim.GetName()
            if not name.endswith("_J"):
                continue

            ops = UsdGeom.Xformable(prim).GetOrderedXformOps()
            key = name.replace("_Link2_J", "").replace("_Link_J", "").replace("_link_J", "")

            if name == "base_link_J":
                for op in ops:
                    n = op.GetOpName().lower()
                    if "rotate" in n:
                        self._base_rotate = op
                    elif "translate" in n and "pivot" not in n:
                        self._base_translate = op
                continue

            if name == "jack_link_J":
                self._jack_op = ops[0]
                continue

            if "grip" in key:
                for op in ops:
                    if "translate" in op.GetOpName().lower():
                        self._grip_ops[key] = op
                        break
                continue

            for op in ops:
                if op.GetOpName() in ("xformOp:rotateY", "xformOp:rotateZ", "xformOp:rotateX"):
                    self._rot_ops[key] = op
                    break

        print(f"[fiibot {self.robot_id}] {self.root}: "
              f"{len(self._rot_ops)} joints, {len(self._grip_ops)} grippers, "
              f"base={'Y' if self._base_rotate else 'N'}, jack={'Y' if self._jack_op else 'N'}")

    def calibrate_here(self):
        if self._earliest_pose is None:
            print(f"[fiibot {self.robot_id}] no pose yet — cannot calibrate")
            return False
        mx, my, mth = self._earliest_pose
        self._aligner.calibrate(mx, my, mth, self.ref["x"], self.ref["y"], self.ref["yaw"])
        print(f"[fiibot {self.robot_id}] calibrated at ref "
              f"({self.ref['x']}, {self.ref['y']}, {self.ref['yaw']})  {self._aligner.describe()}")
        return True

    def reset_calibration(self):
        self._aligner.reset()
        print(f"[fiibot {self.robot_id}] calibration reset")

    def apply(self, payload):
        js = payload.get("joint_states")
        if js:
            self._apply_joints(js.get("name", []), js.get("position", []))

        lg = payload.get("left_gripper")
        if lg:
            self._apply_gripper(lg.get("name", []), lg.get("position", []))

        rg = payload.get("right_gripper")
        if rg:
            self._apply_gripper(rg.get("name", []), rg.get("position", []))

        nav = payload.get("navigation_goal")
        if nav:
            self._apply_pose(nav.get("x"), nav.get("y"), nav.get("theta"))

    def _apply_joints(self, names, positions):
        for joint_name, pos in zip(names, positions):
            key = joint_name.replace("_joint", "")

            if key == "jack" and self._jack_op:
                h = max(0.0, min(0.70, float(pos)))
                self._jack_op.Set(Gf.Vec3d(0, 0, h))
                continue

            op = self._rot_ops.get(key)
            if op:
                deg = math.degrees(float(pos))
                if key in INVERT:
                    deg = -deg
                op.Set(deg)

    def _apply_gripper(self, names, positions):
        for gname, pos in zip(names, positions):
            key = gname.replace("_joint", "")
            op = self._grip_ops.get(key)
            if not op:
                continue
            d = max(GRIP_MIN, min(GRIP_MAX, float(pos)))
            vec = [0.0, 0.0, 0.0]
            vec[GRIP_AXIS] = d
            op.Set(Gf.Vec3d(*vec))

    def _apply_pose(self, x, y, theta):
        if self._base_translate is None or x is None:
            return
        x, y, theta = float(x), float(y), float(theta)
        self._earliest_pose = (x, y, theta)
        X, Y, yaw = self._aligner.to_scene(x, y, theta)
        self._base_translate.Set(Gf.Vec3d(X, Y, 0))
        if self._base_rotate:
            self._base_rotate.Set(Gf.Vec3d(0, 0, math.degrees(yaw)))


class MyExtension(omni.ext.IExt):
    def on_startup(self, _ext_id):
        self._consumer = None
        self._sub = None
        self._window = None
        self.drivers = {}

        stage = omni.usd.get_context().get_stage()
        if stage is None:
            print("[fiibot] no stage open — open scene first, then toggle")
            return

        for robot_id, root in ROBOT_MAP.items():
            self.drivers[robot_id] = RobotDriver(stage, robot_id, root)

        self._consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=BROKER,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
        )
        print(f"[fiibot] subscribed to '{KAFKA_TOPIC}', robots: {list(self.drivers)}")

        self._sub = (omni.kit.app.get_app()
                     .get_update_event_stream()
                     .create_subscription_to_pop(self._on_update, name="fiibot official"))

        self._window = ui.Window("Fiibot Calibration", width=340, height=140)
        with self._window.frame:
            with ui.VStack(spacing=8):
                ui.Label("Drive BOTH robots to their reference points, then:")
                self._status = ui.Label("", word_wrap=True)
                with ui.HStack(spacing=6, height=30):
                    ui.Button("Calibrate All", clicked_fn=self._calibrate_all)
                    ui.Button("Reset All", clicked_fn=self._reset_all)

    def _calibrate_all(self):
        results = []
        for rid, d in self.drivers.items():
            ok = d.calibrate_here()
            results.append(f"{rid}: {'OK' if ok else 'no pose'}")
        self._status.text = "  |  ".join(results)

    def _reset_all(self):
        for d in self.drivers.values():
            d.reset_calibration()
        self._status.text = "all reset"

    def _on_update(self, _e):
        if not self._consumer:
            return
        records = self._consumer.poll(timeout_ms=0)
        if not records:
            return

        earliest = {}
        for _tp, msgs in records.items():
            for m in msgs:
                data = m.value
                for robot_id, payload in data.items():
                    earliest[robot_id] = payload

        for robot_id, payload in earliest.items():
            driver = self.drivers.get(robot_id)
            if driver:
                driver.apply(payload)

    def on_shutdown(self):
        if self._sub:
            self._sub.unsubscribe()
            self._sub = None
        if self._consumer:
            self._consumer.close()
            self._consumer = None
        self.drivers = {}
        self._window = None