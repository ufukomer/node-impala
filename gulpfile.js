const gulp = require('gulp');
const babel = require('gulp-babel');
const thrift = require('gulp-thrift');

const basePaths = {
    src: 'src/',
    dest: 'lib/'
};

const paths = {
    js: {
        src: `${ basePaths.src }thrift-impala.js`,
        dest: basePaths.dest
    },
    thrift: {
        src: `${ basePaths.src }thrift/*.thrift`,
        dest: `${ basePaths.dest }thrift/`
    }
};

/**
 * Runs watch task as default
 */
gulp.task('default', ['watch']);

gulp.task('watch', ['js'], () => {
    // Watch changes on impala.js
    gulp.watch(paths.js.src, ['js']);
});

/**
 * Generate thrift files. This task requires to havethrift installed in the system.
 */
gulp.task('thrift', () => {
    gulp.src(paths.thrift.src)
        .pipe(thrift({
            gen: 'js:node'
        }))
        .pipe(gulp.dest(paths.thrift.dest));
});

/**
 * Copy impala.js from source to destination.
 */
gulp.task('js', () => {
    gulp.src(paths.js.src)
        .pipe(gulp.dest(paths.js.dest));
});
