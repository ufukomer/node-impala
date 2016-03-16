import gulp from 'gulp';
import thrift from 'gulp-thrift';
import babel from 'gulp-babel';

const basePaths = {
  src: 'src/',
  dest: 'lib/',
};

const paths = {
  js: {
    src: `${basePaths.src}*.js`,
    dest: basePaths.dest,
  },
  thrift: {
    src: `${basePaths.src}thrift/*.thrift`,
    dest: `${basePaths.dest}thrift/`,
  },
};

/**
 * Runs watch task as default.
 */
gulp.task('default', ['watch']);

gulp.task('watch', ['compile:js'], () => {
  gulp.watch(paths.js.src, ['compile:js']);
});

/**
 * Generates thrift files. This task requires to have thrift
 * installed in the system.
 */
gulp.task('thrift', () => {
  gulp.src(paths.thrift.src)
    .pipe(thrift({
      gen: 'js:node',
    }))
    .pipe(gulp.dest(paths.thrift.dest));
});

/**
 * Compiles ES6 code to ES5.
 */
gulp.task('compile:js', () => {
  gulp.src(paths.js.src)
    .pipe(babel({
      presets: ['es2015'],
    }))
    .on('error', console.error.bind(console))
    .pipe(gulp.dest(paths.js.dest));
});
