import tensorflow as tf
from djlab.in_out import ArgumentParser


argparser = ArgumentParser()
argparser.add_argument("-size", type=int, default=128)
argparser.add_argument("-dropout", type=float, default=0.2)
argparser.add_argument("-epochs", type=int, default=5)
flags = argparser.parse_args_dj()

mnist = tf.keras.datasets.mnist

(x_train, y_train), (x_test, y_test) = mnist.load_data()
x_train, x_test = x_train / 255.0, x_test / 255.0

model = tf.keras.models.Sequential([
  tf.keras.layers.Flatten(input_shape=(28, 28)),
  tf.keras.layers.Dense(flags.size, activation='relu'),
  tf.keras.layers.Dropout(flags.dropout),
  tf.keras.layers.Dense(10)
])

predictions = model(x_train[:1]).numpy()

tf.nn.softmax(predictions).numpy()

loss_fn = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)

loss_fn(y_train[:1], predictions).numpy()

model.compile(optimizer='adam',
              loss=loss_fn,
              metrics=['accuracy'])

history = model.fit(x_train, y_train, epochs=flags.epochs, validation_data=(x_test, y_test))

evaluation = model.evaluate(x_test,  y_test, verbose=2)

training_logs = {"train_" + metric: value for metric, value in history.history.items()}
test_logs = dict(zip(model.metrics_names, evaluation))

flags.update_log(training_logs)
flags.update_log(test_logs)
flags.done()

