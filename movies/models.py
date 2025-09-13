import uuid
from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.validators import MinValueValidator, MaxValueValidator


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class TimeStampedMixin(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)  
    updated_at = models.DateTimeField(auto_now=True)      

    class Meta:
        abstract = True


class FilmWorkType(models.TextChoices):
    MOVIE = 'movie', _('Фильм')
    TV_SHOW = 'tv_show', _('ТВ шоу')


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    class Meta:
        db_table = 'content"."genre'
        verbose_name = _('Жанр')
        verbose_name_plural = _('Жанры')

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.TextField(_('full name'))

    class Meta:
        db_table = 'content"."person'
        verbose_name = _('Участник')
        verbose_name_plural = _('Участники')

    def __str__(self):
        return self.full_name


class FilmWork(UUIDMixin, TimeStampedMixin):
    title = models.TextField(_('title'))
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateField(_('creation date'), blank=True, null=True)
    rating = models.FloatField(
        _('rating'), 
        blank=True, 
        null=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)]
    )
    type = models.TextField(_('type'), choices=FilmWorkType.choices)
    file_path = models.TextField(_('file path'), blank=True, null=True)

    class Meta:
        db_table = 'content"."film_work'
        verbose_name = _('Фильм')
        verbose_name_plural = _('Фильмы')

    def __str__(self):
        return self.title


class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)  

    class Meta:
        db_table = 'content"."genre_film_work'
        verbose_name = _('Жанр фильма')
        verbose_name_plural = _('Жанры фильмов')
        unique_together = [['film_work', 'genre']]

    def __str__(self):
        return f'{self.film_work.title} - {self.genre.name}'


class PersonFilmWork(UUIDMixin):
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.TextField(_('role'))
    created_at = models.DateTimeField(auto_now_add=True)  

    class Meta:
        db_table = 'content"."person_film_work'
        verbose_name = _('Участник фильма')
        verbose_name_plural = _('Участники фильмов')
        unique_together = [['film_work', 'person', 'role']]

    def __str__(self):
        return f'{self.person.full_name} - {self.role} - {self.film_work.title}'


FilmWork.add_to_class(
    'genres',
    models.ManyToManyField(Genre, through=GenreFilmWork, verbose_name=_('жанры'))
)

FilmWork.add_to_class(
    'persons', 
    models.ManyToManyField(Person, through=PersonFilmWork, verbose_name=_('участники'))
)